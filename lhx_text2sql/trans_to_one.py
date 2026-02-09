import os

# ================= 配置区域 =================

# 1. 输出的汇总文件名
OUTPUT_FILENAME = "project_context_for_ai.txt"

# 2. 需要读取的文件后缀 (白名单模式，只读取代码和配置文件)
# 根据你的项目需求可以自由添加，例如 .c, .cpp, .java, .sql 等
ALLOWED_EXTENSIONS = {
    # Python
    '.py', 
    # Web / JS
    '.js', '.jsx', '.ts', '.tsx', '.vue', '.html', '.css', '.scss', '.json',
    # 配置 / 文档
    '.xml', '.yaml', '.yml', '.md', '.txt', '.ini', '.conf', '.env'
}

# 3. 需要忽略的目录 (完全跳过，不遍历内部)
IGNORE_DIRS = {
    '.git', '.svn', '.hg', '.idea', '.vscode', 
    '__pycache__', 'node_modules', 'venv', 'env', '.venv',
    'dist', 'build', 'coverage', 'migrations'
}

# 4. 需要忽略的具体文件名
IGNORE_FILES = {
    OUTPUT_FILENAME, 'merge_to_one_file.py', 'package-lock.json', 'yarn.lock'
}

# ===========================================

def is_allowed_file(filename):
    """检查文件后缀是否在白名单中，且不在忽略列表中"""
    if filename in IGNORE_FILES:
        return False
    _, ext = os.path.splitext(filename)
    return ext.lower() in ALLOWED_EXTENSIONS

def generate_tree(start_path):
    """生成目录树字符串"""
    tree_str = []
    for root, dirs, files in os.walk(start_path):
        # 过滤忽略的目录
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        
        level = root.replace(start_path, '').count(os.sep)
        indent = ' ' * 4 * (level)
        tree_str.append(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            if is_allowed_file(f):
                tree_str.append(f"{subindent}{f}")
    return "\n".join(tree_str)

def merge_files():
    root_dir = os.getcwd()
    output_path = os.path.join(root_dir, OUTPUT_FILENAME)
    
    print(f"🚀 开始合并代码...")
    print(f"📂 扫描目录: {root_dir}")
    
    merged_content = []
    
    # 1. 写入项目结构树
    print("🌳 生成项目结构...")
    tree = generate_tree(root_dir)
    merged_content.append("=" * 50)
    merged_content.append("PROJECT STRUCTURE (项目结构)")
    merged_content.append("=" * 50)
    merged_content.append(tree)
    merged_content.append("\n\n")

    # 2. 遍历并读取文件内容
    file_count = 0
    
    for root, dirs, files in os.walk(root_dir):
        # 修改 dirs 列表以跳过忽略的目录
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        
        for file in files:
            if not is_allowed_file(file):
                continue
                
            file_path = os.path.join(root, file)
            rel_path = os.path.relpath(file_path, root_dir)
            
            try:
                # 尝试以 UTF-8 读取
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                # 格式化写入：添加清晰的文件头
                header = f"\n\n{'='*50}\nFILE PATH: {rel_path}\n{'='*50}\n"
                merged_content.append(header)
                merged_content.append(content)
                
                print(f"   + 读取: {rel_path}")
                file_count += 1
                
            except UnicodeDecodeError:
                print(f"⚠️  跳过 (编码非UTF-8): {rel_path}")
            except Exception as e:
                print(f"❌ 读取错误 {rel_path}: {e}")

    # 3. 写入最终文件
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("".join(merged_content))
        
        print("-" * 30)
        print(f"✅ 合并完成！")
        print(f"📄 共合并文件数: {file_count}")
        print(f"💾 输出文件: {OUTPUT_FILENAME}")
        print("👉 你可以直接打开该文件，全选复制发送给 AI。")
        
    except Exception as e:
        print(f"❌ 写入输出文件失败: {e}")

if __name__ == '__main__':
    merge_files()