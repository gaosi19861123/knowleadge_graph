import yaml
import os

def load_config():
    # 获取当前脚本的绝对路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建配置文件的绝对路径
    config_path = os.path.join(current_dir, '..', 'conf', 'config.yaml')
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config

def load_config_from_file():
    # 加载配置
    config = load_config()
    
    # 使用配置参数
    host = config['base']['host']
    port = config['base']['port']
    
    # ... 其余代码 ...
    return host, port

if __name__ == "__main__":
    load_config_from_file()