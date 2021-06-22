from os.path import join
import subprocess as sub
import pandas as pd


def conversion(in_dir, tif_file, newname):
    """ Takes in an .tif file used as topographical data for a PALM run, uses the gdal
        package to convert the file to .asc format. The header is then removed from the file
        and the filename is changed to file_topo in accordance to PALM naming convention."""
    file = join(in_dir, tif_file)
    tif_suffix = file
    asc_suffix = file[:-3] + 'asc'
    s = sub.getstatusoutput(f'gdal_translate {tif_suffix} {asc_suffix}')
    print(s)
    data = pd.read_csv(asc_suffix)
    data = data[5:]
    outname = join(in_dir, newname + '_topo') 
    data.to_csv(outname, index=False, header=None)
    print(f'Conversion finished. Outputted files to {in_dir}')



def main():
    """ Here the input directory is where the files are stored, the tif_file is the name of the .tif file and
        new_file_name is the new file name of the output file. It will be saved as new_file_name_topo."""
    in_dir = r'/home/shaun/Documents/work_dir/greg_project/palm_files/test'
    tif_file = r'DSM_BA32_3603_2013.tif'
    new_file_name = r'testfile'
    conversion(in_dir, tif_file, new_file_name)

if __name__ == "__main__":
    main()