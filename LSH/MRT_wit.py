from hashlib import sha256
import math
import time

class MRTreeNode:
    def __init__(self, acc, left, right):
        self.acc = acc
        self.left = left
        self.right = right
    
    def __str__(self):
        return f'({self.acc}, {self.mul}), '


def build_MRT_tree_from_list(data):
    store_size = 0
    current = []
    # cost = []
    for d in data:
        acc = sha256(b''.join(d)).digest()
        current.append(MRTreeNode(acc, None, None))
        store_size += len(acc)
    while len(current) != 1:
        if len(current) % 2 == 1:
            current.append(MRTreeNode(b'', None, None))
        next = []
        i = 0
        while i != len(current):
            left = current[i]
            right = current[i + 1]
            # t1 = time.time()
            acc = sha256((left.acc+right.acc)).digest()
            # t2 = time.time()
            # cost.append(t2 - t1)
            # print("multi:", left.acc, right.mul, acc)
            leaf = MRTreeNode(acc, left, right)
            store_size += len(acc)
            next.append(leaf)
            # print(left, right, leaf)
            i = i + 2
        current = next
    # print("single hash:", sum(cost) / len(cost) * 1000)
    return current[0], store_size


def cal_MRT_wit(root, idx, tree_size):
    idx = idx - 1
    b_bits = bin(idx)[2:].zfill(math.ceil(math.log(tree_size, 2)))
    leaves = []
    current = root
    for character in b_bits:
        if current is None:
            print("current is none")
            break
        if character == 1:
            leaves.append(current.left)
            current = current.right
        else:
            leaves.append(current.right)
            current = current.left
    return leaves, current.acc


def MRT_verify(wit, d):
    acc = d
    for w in reversed(wit):
        acc = sha256(acc + w.acc).digest()
    return acc

