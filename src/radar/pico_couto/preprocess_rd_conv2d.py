import os
import pandas as pd
import numpy as np
import cv2
import tensorflow as tf
from tensorflow.keras import layers, models

PATH = "..\\data\\nova_base\\img\\"
COORDS_FILE = "..\\data\\nova_base\\estacoes_pluviometricas.csv"
OUTPUT_FILE = "..\\data\\nova_base\\FEATURE_A652_CONV2D.csv"
IMAGE_SIZE = (250, 250)

def create_conv_model():
    model = models.Sequential()
    model.add(layers.Conv2D(16, (3, 3), activation='relu', padding='same', input_shape=(*IMAGE_SIZE, 3)))
    model.add(layers.MaxPooling2D((3, 3)))
    model.add(layers.Dropout(0.25))
    model.add(layers.Conv2D(32, (3, 3), activation='relu', padding='same'))
    model.add(layers.MaxPooling2D((3, 3)))
    model.add(layers.Dropout(0.25))
    model.add(layers.GlobalAveragePooling2D())
    return model

def processing(model):
    df_estacoes = pd.DataFrame()

    for year_folder in os.listdir(PATH):
        year_path = os.path.join(PATH, year_folder)
        if os.path.isdir(year_path):
            for month_folder in os.listdir(year_path):
                month_path = os.path.join(year_path, month_folder)
                if os.path.isdir(month_path):
                    for day_folder in os.listdir(month_path):
                        day_path = os.path.join(month_path, day_folder)
                        if os.path.isdir(day_path):
                            for filename in os.listdir(day_path):
                                print(filename)
                                if filename.endswith(".png"):
                                    file = os.path.join(day_path, filename)
                                    image = cv2.imread(file)
                                    if image is None:
                                        continue
                                    image = cv2.resize(image, IMAGE_SIZE)
                                    if image is None:
                                        continue
                                    image = np.expand_dims(image, axis=0)
                                    feature_maps = model.predict(image)
                                    feature_means = np.mean(feature_maps, axis=(1, 2))
                                    feature_vars = np.var(feature_maps, axis=(1, 2))
                                    df_row = pd.DataFrame([np.concatenate((feature_means.flatten(), feature_vars.flatten()))],
                                                          columns=[f"Mean_{i}" for i in range(feature_means.shape[1])] +
                                                                  [f"Var_{i}" for i in range(feature_vars.shape[1])])
                                    df_estacoes = pd.concat([df_estacoes, df_row])

    df_estacoes.to_csv(OUTPUT_FILE)

if __name__ == '__main__':
    model = create_conv_model()
    processing(model)