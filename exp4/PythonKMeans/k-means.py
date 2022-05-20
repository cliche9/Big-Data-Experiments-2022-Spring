from math import sqrt
import numpy as np
from numpy import linalg
from tqdm import trange
import cv2
import argparse

class Kmeans(object):

    def __init__(self, source_image, k=16):
        self.training_x = source_image.copy().reshape(-1, 3)
        self.label = np.zeros(self.training_x.shape[0])
        self.mu = np.random.randint(low=0, high=255, size=(k, 3)).astype(np.float64)
        self.k = k

    def train(self):
        x = self.training_x
        epsilon = 1e-5
        loop_count = 0
        # 最多loop 100次
        for _ in trange(100):
            loop_count += 1
            pre_mu = self.mu.copy()
            # 计算每个sample对应的group
            for i, xi in enumerate(x):
                # 选取对于sample xi来说，最近的group作为label，距离度量使用L2-Norm ^ 2
                self.label[i] = np.argmin(linalg.norm(xi - self.mu, axis=1, keepdims=True) ** 2)
            # 更新每个group的 mu_j 对应的颜色
            for j in range(self.k):
                # 判断group j是否有数据点
                if (self.label == j).any():
                    self.mu[j] = np.sum(x[self.label == j], axis=0) / np.sum(self.label == j)

            if (linalg.norm(self.mu - pre_mu, axis=1, keepdims=True) < epsilon).all():
                break

        return loop_count

    def reassign(self, img):
        new_img = img.copy().astype(np.float64)
        for i in range(img.shape[0]):
            for j in range(img.shape[1]):
                k = np.argmin(linalg.norm(new_img[i][j] - self.mu, axis=1, keepdims=True) ** 2)
                new_img[i][j] = self.mu[k]

        return new_img

def serialize(img, output):
    print(img.shape)
    serialized_info = img.copy().reshape(-1, 3);
    with open(output, 'w') as f:
        for i in range(serialized_info.shape[0]):
            for color in serialized_info[i]:
                f.write(str(color) + ' ')
            f.write('\n')

def deserialize(input, output):
    img = []
    with open(input, 'r') as f:
        for line in f:
            colors = line.split()
            img.append(colors)
    
    width = int(sqrt(len(img)))
    img = np.array(img).reshape(width, width, 3).astype(np.uint8)
    cv2.imwrite(output, img)

def serialization(img, color_bit=8):
    print('serializing original tiff img info pixel color txt...')
    serialize(img, args.output + f"bird_large_serialized_{color_bit}bits.txt")
    print('deserializing pixel color txt to tiff img...')
    deserialize(args.output + f"bird_large_serialized_{color_bit}bits.txt", args.output + f"bird_large_deserialized_{color_bit}bits.tiff")
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='process some log messages, storing them and signaling a rest server'
    )
    parser.add_argument(
        '--appname',
        required=True,
        help='the name of the spark application (default: SparkPageRank)',
        default='SparkPageRank'
    )
    parser.add_argument(
        '--input',
        required=True,
        help='the input path of data for KMeans'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='the output path of KMeans result'
    )
    args = parser.parse_args()

    large_img = cv2.imread(args.input + "bird_large.tiff")
    small_img = cv2.imread(args.input + "bird_small.tiff")

    # 序列化/反序列化原始图像
    serialization(large_img, 8)
    
    # K-Means
    print('converting 8 bit colors to 4 bit colors ...')
    kmeans = Kmeans(small_img, k=16)
    kmeans.train()
    new_img = np.uint8(np.round(kmeans.reassign(large_img)))
    # cv2.imwrite(args.output + "bird_large_4bits.tiff", ))

    # 序列化/反序列化颜色压缩图像
    serialization(new_img, 4)