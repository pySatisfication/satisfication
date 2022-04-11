### Requirements: software
1. python: `python 3.X`
2. packages: `numpy`, `matplotlib`

### Run unit tests
```Shell
# perform robustness testing
python test.py
```

### Run functional test
```Shell
# test single ema func
python util.py ema
python util.py slope

# test multiple funcs
python util.py ma sma ema

# test all func
python util.py all
```

