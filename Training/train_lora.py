import os
import torch
from datasets import load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainingArguments,
    logging,
)
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training
from trl import SFTTrainer

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
# We use the base model Defog fine-tuned, which is excellent for SQL
MODEL_NAME = "defog/llama-3-sqlcoder-8b" 
NEW_MODEL_NAME = "llama-3-8b-census-sql-adapter"

# Path to the consolidated data from Phase 2
TRAIN_DATA_PATH = "../Pre-Process/consolidated_train.jsonl" 
OUTPUT_DIR = "./results"

# LoRA Parameters (Optimized for SQL reasoning)
LORA_R = 32             # Rank: Higher = more parameters to train (16-64 is standard)
LORA_ALPHA = 64         # Scaling factor (usually 2x Rank)
LORA_DROPOUT = 0.05

# Training Parameters
# 1 epoch is often enough for ~600 highly specific examples to avoid overfitting
NUM_EPOCHS = 1          
BATCH_SIZE = 2          # Keep low (1 or 2) for T4/Consumer GPUs
GRAD_ACCUMULATION = 4   # Simulates a larger effective batch size (e.g. 2 * 4 = 8)
LEARNING_RATE = 2e-4

def main():
    print(f"🚀 Initializing Training for {MODEL_NAME}...")

    # 1. Quantization Config (4-bit loading)
    # This is crucial to fit the 8B model onto a T4 GPU (16GB VRAM) or Consumer GPU (8GB+)
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.float16,
        bnb_4bit_use_double_quant=False,
    )

    # 2. Load Base Model
    print("   ⬇️  Loading base model...")
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        quantization_config=bnb_config,
        device_map="auto",
        use_cache=False # Silence warnings during training
    )
    model.config.pretraining_tp = 1

    # 3. Load Tokenizer
    print("   ⬇️  Loading tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "right"

    # 4. Load Dataset
    print(f"   📂 Loading dataset from {TRAIN_DATA_PATH}...")
    if not os.path.exists(TRAIN_DATA_PATH):
        print(f"   ❌ ERROR: Training data not found at {TRAIN_DATA_PATH}")
        print("      Please run 'Pre-Process/consolidate_outputs.py' first.")
        return
    
    dataset = load_dataset("json", data_files=TRAIN_DATA_PATH, split="train")
    print(f"   ✅ Loaded {len(dataset)} training examples.")

    # 5. LoRA Configuration
    peft_config = LoraConfig(
        r=LORA_R,
        lora_alpha=LORA_ALPHA,
        lora_dropout=LORA_DROPOUT,
        bias="none",
        task_type="CAUSAL_LM",
        # Targeting all linear layers significantly improves complex reasoning capabilities
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"] 
    )

    # 6. Training Arguments
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        num_train_epochs=NUM_EPOCHS,
        per_device_train_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=GRAD_ACCUMULATION,
        optim="paged_adamw_32bit", # Paged optimizer saves memory
        save_steps=25,
        logging_steps=5,
        learning_rate=LEARNING_RATE,
        weight_decay=0.001,
        fp16=False,
        bf16=False, # Set to True ONLY if using A100/L4/RTX 40-series
        max_grad_norm=0.3,
        warmup_ratio=0.03,
        group_by_length=True,
        lr_scheduler_type="constant",
        report_to="none" # Disable WandB/Tensorboard for simple runs
    )

    # 7. Initialize Trainer (SFT - Supervised Fine-Tuning)
    trainer = SFTTrainer(
        model=model,
        train_dataset=dataset,
        peft_config=peft_config,
        dataset_text_field="text", # The column name in your JSONL
        max_seq_length=2048,       # Llama 3 context window
        tokenizer=tokenizer,
        args=training_args,
        packing=False,
    )

    # 8. Train
    print("\n🔥 Starting Training... (This may take 30-60 mins depending on GPU)")
    trainer.train()
    
    # 9. Save the Adapter
    print(f"\n💾 Saving adapter to ./{NEW_MODEL_NAME}...")
    trainer.model.save_pretrained(NEW_MODEL_NAME)
    tokenizer.save_pretrained(NEW_MODEL_NAME)
    
    print("\n🎉 Training Complete! Adapter saved.")
    print(f"   You can now move to Phase 4: Integration.")

if __name__ == "__main__":
    main()