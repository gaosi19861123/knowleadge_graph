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

class Config():
    def __init__(self):
        self.cfg = load_config()
        self._pos_cfg() 
        self._node_relation()

    def _pos_cfg(self):
        # 使用配置参数
        self.tbl_sales_fp = self.cfg['pos_data']['tbl_sales_fp']
        self.query_period = self.cfg['pos_data']['query_period']
        self.campaign_start_date = self.cfg['pos_data']['campaign_start_date']
        self.pos_col = self.cfg['pos_data']['col']  
        self.comp_cd = self.cfg['pos_data']['comp_cd']
    
    def _node_relation(self):
        self.node_fp = self.cfg['node_relation']['node_json_fp']
        self.relation_fp = self.cfg['node_relation']['relation_json_fp']

if __name__ == "__main__":
    pass