from pathlib import Path
from hl7apy.parser import parse_message
from hl7apy.consts import VALIDATION_LEVEL

def get_obx_segments(hl7_msg):
    """
    Extract OBX segments from a HL7 message string
    
    Parameters
    ------------------------------------
    hl7_msg:str
        hl7 message saved in a txt file

    Returns
    ------------------------------------
    obx_lines: list
        list of OBX lines present in the mesdsage
    """
    obx_lines = []
    for line in hl7_msg.strip().splitlines():
        line = line.strip()
        if line.startswith("OBX|"):
            obx_lines.append(line)
    return obx_lines


# Directories
results_dir = Path("./results_msg")
response_dir = Path("./responses_stitch")
output_dir = Path("./merged")
output_dir.mkdir(exist_ok=True)

response_names = [f.name for f in response_dir.iterdir() if f.is_file()]

# Go over the files in the results directory 
#check if the name matches with the ones in the response directory
for result_file in results_dir.iterdir():
    
    result_name = result_file.name
    if result_name not in response_names:
        continue

    print(f"Matching file found: {result_name}")

    result_text = result_file.read_text()
    response_text = (response_dir/result_name).read_text()

    # Get OBX segments
    result_obx_lines = get_obx_segments(result_text)
    response_obx_lines = get_obx_segments(response_text)

    if not response_obx_lines:
        print(f"No OBX segments found in {result_name}, skipping.")
        continue

    # Find next OBX index
    next_index = len(result_obx_lines) + 1

    # Append new OBX segments with updated numbering
    new_obx_lines = []
    for line in response_obx_lines:
        parts = line.split('|')
        parts[1] = str(next_index)
        new_obx_lines.append('|'.join(parts))
        next_index += 1

    # Insert before SPM or ZSP (keep message order)
    lines = result_text.strip().splitlines()
    insert_idx = len(lines)
    for i, line in enumerate(lines):
        if line.startswith("SPM|") or line.startswith("ZSP|"):
            insert_idx = i
            break

    # Merge everything together
    merged_lines = lines[:insert_idx] + new_obx_lines + lines[insert_idx:]

    output_file = output_dir / result_name
    output_file.write_text("\n".join(merged_lines))

    print(f"Merged message written to {output_file}\n")