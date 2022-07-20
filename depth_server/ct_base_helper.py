import sys
from easydict import EasyDict as edict

class CTBaseHelper(object):
    def __init__(self):
        self.conf_file = '../conf/ct_base_conf'
        self._code_base = {}
        self.init_conf()

    def get_base_d(self, code_prefix):
        key_lower = code_prefix.lower()
        if key_lower in self._code_base:
            return self._code_base[key_lower]
        return None

    def init_conf(self):
        with open(self.conf_file, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                if len(line) == 0:
                    continue

                items = line.split('\t')
                assert len(items) >= 8
                code_pre_lower = items[3].lower()

                d_item = edict(e_name=items[0],
                               e_code=items[1],
                               c_name=items[2],
                               code_prefix=code_pre_lower,
                               category=items[4],
                               q_per_hand=int(items[5]),
                               trade_unit=items[6],
                               min_change_price=items[7],
                               ud_limit=items[8])
                self._code_base[code_pre_lower] = d_item
