import requests
import numpy as np
import cv2
while True:
    images=requests.get("192.168.0.104:8080/shot.jpg")
    video=np.array(bytearray(images.content),dtype=np=uint8)
    render = cv2.imdecode(video,-1)
    cv2.imshow('frame', render)
    if(cv2.waitkey(1) and oxff==ord('q')):
        break