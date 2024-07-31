from hashlib import sha256
from datasketch import MinHashLSH, MinHash
import math
import struct


BIT_STRING_LENGTH = 32 * 8


class LSHTreeNode:
    def __init__(self, acc, left, right):
        self.acc = acc
        self.left = left
        self.right = right
    
    def __str__(self):
        return f'({self.acc}), '


def cal_minhash_bits(chunks):
    minhash = MinHash(num_perm=8)
    for chunk in chunks:
        minhash.update(chunk)
    result = b''
    for num in minhash.digest():
        num_bytes = struct.pack('>I', num)
        result += num_bytes
    bit_str = ''.join(format(byte, '08b') for byte in result)
    # print(int_2_bit_str(hash_num))
    return bit_str


def int_2_bit_str(x, length=BIT_STRING_LENGTH):
    return bin(x)[2:].zfill(length)


def cal_level_lsh_delta(lsh_values):
    current = lsh_values
    while len(current) != 1:
        next_values = []
        idx = 0
        while idx < len(current):
            left = current[idx]
            right = left
            if idx + 1 != len(current):
                right = current[idx + 1]
            idx += 2
            if len(current) == len(lsh_values):
                next_values.append(left ^ right)
            else:
                next_values.append(left | right)
        current = next_values
    # print("delta:", int_2_bit_str(current[0]), count_0_in_bits(current[0]))
    return current[0]


def build_LSH_tree_from_list(data):
    store_size = 0
    current = []
    lsh_values = []
    for chunks in data:
        lsh_bits = cal_minhash_bits(chunks)
        lsh_values.append(int(lsh_bits, 2))
        current.append(LSHTreeNode(lsh_bits, None, None))
    delta_value = cal_level_lsh_delta(lsh_values)
    hash_length = len(int_2_bit_str(lsh_values[0]))
    same_bit_count = count_0_in_bits(delta_value, hash_length)
    if hash_length != BIT_STRING_LENGTH: print("length not equal", hash_length, int_2_bit_str(lsh_values[0]))
    if len(lsh_values) <= 2:
        store_size += hash_length * len(lsh_values)
    else:
        store_size += (hash_length + BIT_STRING_LENGTH + (len(lsh_values) - 1) * (hash_length - same_bit_count))
    
    while len(current) != 1:
        next_values = []
        i = 0
        lsh_values = []
        while i < len(current):
            left = current[i]
            if i + 1 != len(current): 
                right = current[i + 1]
            else:
                right = current[i-1]
            chunk = [left.acc.encode()[i:i+8] for i in range(0, len(left.acc.encode()), 8)]
            chunk.extend([right.acc.encode()[i:i+8] for i in range(0, len(right.acc.encode()), 8)])
            lsh_bits = cal_minhash_bits(chunk) 
            leaf = LSHTreeNode(lsh_bits, left, right)
            next_values.append(leaf)
            lsh_values.append(int(lsh_bits, 2))
            i += 2
        delta_value = cal_level_lsh_delta(lsh_values)
        hash_length = len(int_2_bit_str(lsh_values[0]))
        same_bit_count = count_0_in_bits(delta_value, hash_length)
        if hash_length != BIT_STRING_LENGTH: print("length not equal", hash_length, int_2_bit_str(lsh_values[0]))
        if len(current) <= 2:
            store_size += hash_length * len(lsh_values)
        else:
            store_size += (hash_length + BIT_STRING_LENGTH + (len(lsh_values) - 1) * (hash_length - same_bit_count))
        current = next_values
    
    return current[0], store_size/8


def cal_LSH_wit(root, idx, tree_size):
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


def count_0_in_bits(xdelta, length=BIT_STRING_LENGTH):
    binary_string = bin(xdelta)[2:].zfill(length)
    count_same = 0
    for x in binary_string:
        if x == '0': count_same += 1
    return count_same
