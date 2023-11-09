from copy import deepcopy


def gen_comb_list(lists):
    result = []

    check_val = 1

    for ls in lists:
        if len(ls) != 1:
            check_val *= -1
            break

    if check_val > 0:
        temp = []
        for x in lists:
            temp.append(x[0])
        return temp
    else:
        for i in range(len(lists)):
            if len(lists[i]) != 1:
                for j in lists[i]:
                    copy_lsts = deepcopy(lists)
                    copy_lsts[i] = [j]
                    new_comb = gen_comb_list(copy_lsts)
                    if isinstance((new_comb[0]), list):
                        result += new_comb
                    else:
                        result.append(new_comb)
                return result

