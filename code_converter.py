def code_to_isin(code):
    val_list = [40, 47, 14] + [x * y for x, y in zip([int(ch) for ch in code], [1, 2, 1, 2, 1, 2])] + [0, 0]
    check_val = sum([int(v/10) + v % 10 for v in val_list]) % 10
    check_str = '0'
    if check_val > 0:
        check_str = str(10 - check_val)
    return 'KR7' + code + '00' + check_str


def isin_to_code(isin):
    return isin[3:9]
