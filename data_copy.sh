#!/bin/bash

# Define your source and destination directories
SRC_DIR="/Users/saraalam/Desktop/DPOpt_broken/tpch_data"
DEST_DIR="/Users/saraalam/Desktop/DPOpt/tpch_data"

# Create destination if it doesn't exist
mkdir -p "$DEST_DIR"

for file in "customer" "lineitem" "nation" "orders" "part" "partsupp" "region" "supplier"; do
    
    INPUT_FILE="$SRC_DIR/$file.csv"

    if [ -f "$INPUT_FILE" ]; then
        
        # Split the source file into 2 parts (creates temp_split_a and temp_split_b)
        # We run this in the current directory to avoid path issues
        #split -n 2 -a 1 "$INPUT_FILE" "temp_split_"
        # Count total lines
        TOTAL_LINES=$(wc -l < "$INPUT_FILE")

        # Compute half
        HALF_LINES=$(( (TOTAL_LINES + 1) / 2 ))

        # Split by lines
        split -l "$HALF_LINES" -a 1 "$INPUT_FILE" "temp_split_"

        # Move and rename the parts to the NEW destination directory
        mv "temp_split_a" "$DEST_DIR/alice_$file.csv"
        mv "temp_split_b" "$DEST_DIR/bob_$file.csv"

        echo "✅ Split $file.csv from broken -> alice/bob in DPOpt"
    else
        echo "❌ File not found in source: $INPUT_FILE"
    fi
done