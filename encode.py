import os

def convert_py_files(root_dir='.', output_dir='txt_files', extension='.py.txt'):
    """
    Converts all .py files in the specified directory to .py.txt format.
    Creates files in a flat structure in output directory with paths as filenames.
    
    Args:
        root_dir (str): Root directory to search for .py files
        output_dir (str): Directory to store converted files
        extension (str): Extension for the output files (default: .py.txt)
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all .py files recursively
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                
                # Read the original file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read()
                
                # Get relative path from root_dir
                rel_path = os.path.relpath(file_path, root_dir)
                
                # Use the relative path as the filename (replacing / with _)
                output_filename = rel_path.replace(os.sep, '_')
                output_file_path = os.path.join(output_dir, output_filename + extension)
                
                # Save the content with .py.txt extension
                with open(output_file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"Converted: {file_path} -> {output_file_path}")

if __name__ == "__main__":
    # Use 'fbdi' directory as source
    directory = 'fbdi'
    
    # Store converted files in 'txt_files' directory
    output_directory = 'txt_files'
    
    # Use .py.txt extension for output files
    output_extension = '.txt'
    
    convert_py_files(directory, output_directory, output_extension)
    print("Conversion complete.")