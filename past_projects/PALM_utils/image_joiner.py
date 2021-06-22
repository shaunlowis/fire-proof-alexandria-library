""" Script for horizontally joining two VAPOR saved images
    Written by Johnny (Shaun) Lowis for Bodeker Scientific."""

import os
import PIL
import numpy as np
from PIL import Image
import pandas as pd


def join_im(dir_left, dir_right, outdir):
    """ Takes in image files from two directories and outputs the two images joined horizontally in an output
        directory.
    :param dir_left: directory of all the images that should go on the left of the output image
    :param dir_right: directory of all the images that should go on the left of the output image
    :param outdir: directory for outputting the joined images to.
    """
    # Making lists of the files in the two input directories:
    left_files = [os.path.join(dir_left, f) for f in os.listdir(dir_left) if os.path.isfile(os.path.join(dir_left, f))]
    right_files = \
        [os.path.join(dir_right, f) for f in os.listdir(dir_right) if os.path.isfile(os.path.join(dir_right, f))]

    left_files = sorted(left_files)
    right_files = sorted(right_files)
    # Iterating through the lists made above, can add in a sorting method later
    for i, file in enumerate(right_files):
        # Opening both image files
        left_im = PIL.Image.open(left_files[i])
        right_im = PIL.Image.open(file)

        # Crop the images. If in Georeferenced view mode in VAPOR, these coordinates should give a good crop.
        l_width, l_height = left_im.size
        x, y = 428, 39
        left_im = left_im.crop((x, y, l_width - x, l_height - y))

        r_width, r_height = right_im.size
        x, y = 428, 15
        right_im = right_im.crop((x, y, r_width - x, r_height - y))

        # Resize images to fit the smaller of the two:
        min_shape = sorted([(np.sum(i.size), i.size) for i in [left_im, right_im]])[0][1]

        right_im = right_im.resize(left_im.size)

        # Horizontally appending the images
        combined = np.hstack((np.asarray(i.resize(min_shape)) for i in [left_im, right_im]))
        imgs_comb = PIL.Image.fromarray(combined)

        # Setting the black areas of the combined image to transparent
        imgs_comb = imgs_comb.convert("RGBA")
        data = imgs_comb.getdata()

        newData = []
        for item in data:
            if item[0] == 000000 and item[1] == 000000 and item[2] == 000000:
                newData.append((255, 255, 255, 0))
            else:
                newData.append(item)

        imgs_comb.putdata(newData)

        # Saving to output directory
        imgs_comb.save(os.path.join(outdir, f'joined{i}.png'))
        print(f'Generated Image number {i} at {outdir}')
        
        
def add_ramp(infile, axis, side):
    """ Add a gradient ramp on either the x (0) or y (1) axis:
        :param infile: input .tiff file
        :param axis: x = 0, y = 1
        :param side: Whether to append the ramp to the left or right side of the .tiff 
                     'L' = left, 'R' = right, when side = 'L' and axis = 0, appends to top.
                     When side = 'R', axis = 0, appends to bottom.
    """
    data = PIL.Image.open(infile)
    data_arr = np.array(data)
    ones = np.ones(data_arr.shape)
    
    if side == 'L':
        if axis == 1:
            for i in range(0, data_arr.shape[1]):
                ones[:,i] = i * 0.1
            comb = np.concatenate((ones, data_arr), axis=1)
        else:
            for i in range(0, data_arr.shape[0]):
                ones[i,:] = i * 0.1
            comb = np.concatenate((ones, data_arr), axis=0)
    else:
        if axis == 1:
            for i in range(0, data_arr.shape[1]):
                ones[:,-i] = i * 0.1
            comb = np.concatenate((data_arr, ones), axis=1)
        else:
            for i in range(0, data_arr.shape[0]):
                ones[-i,:] = i * 0.05
            comb = np.concatenate((data_arr, ones), axis=0)
    
    comb = PIL.Image.fromarray(comb)
    comb.show()


def main():
    """ This script uses the OS module so should work for Windows and Linux."""
    dir_left = r'/home/shaun/Documents/work_dir/greg_project/data_visualisation/VAPOR_vis/uv/with_building'
    dir_right = r'/home/shaun/Documents/work_dir/greg_project/data_visualisation/VAPOR_vis/uv/without_building'
    outdir = r'/home/shaun/Documents/work_dir/greg_project/data_visualisation/VAPOR_vis/uv/combined'
    join_im(dir_left, dir_right, outdir)
    
#     infile = r'/home/shaun/Documents/work_dir/greg_project/palm_files/skytower/dead_buildings/deader_building.tif'
#     add_ramp(infile, 0, 'R')


main()
