"""This is a script for taking in arg data and creating plots based off of netCDF files.
   The script will be amended for plotting ozone files once that database is finished.
   Author: Shaun Lowis, Date: July 11, 2019, for Bodeker Scientific."""

from netCDF4 import Dataset
from matplotlib import pyplot as plt
import numpy as np
from matplotlib.font_manager import FontProperties
import netCDF4 as nc
import matplotlib.colors as colors
import argparse


from bdbp.utils.paths import default_mmzm_path, create_mmzm_fname
from bdbp.collection import ProfileCollection


class Plot:
    """The plot input of the user will be in the form of a class. This class has the following
       attributes; 'data', 'neg_data' and 'datatype'"""

    def __init__(self, data, neg_data, datatype):
        """Here follows the attributes of the Plot class, data is used to make the plots variable, allowing
           the user to plot different datatypes eg. NO3 and H2O with one arg change. Neg data and datatype
           are effectively used as globals, as these values are changed locally in the plots, but should
           remain constant and immutable in the file and program.
        """

        self.data = data
        self.neg_data = neg_data
        self.datatype = datatype

    def lat_vs_year(self, x_var, y_var, loop_var, loop_count):
        """Makes a plot of latitude vs year. loop_var is used to determine which variable is used to
           loop over the data with, and loop count is used to iterate through the data. The other
           plots also run on this principle and since these are inferred values, they are generated
           in main() if changes are desired. x_var and y_var are arguments that can be changed during
           initialisation of the script This plot is linear whereas the other two are logarithmic.
           This is because it does not have a change in levels.
        """

        fig, ax = plt.subplots(1, 1, figsize=(18, 12))
        font = FontProperties()
        font.set_size(16)
        ax.use_sticky_edges = True

        if self.datatype == "HNO3_mean":
            plt.title(r"$HNO_3$ monthly mean zonal mean at {:.2f} hPa".format(loop_var[loop_count]),
                      fontproperties=font)
        elif self.datatype == "H2O_mean":
            plt.title(r"$H_2$O monthly mean zonal mean at {:.2f} hPa".format(loop_var[loop_count]),
                      fontproperties=font)

        plt.xlabel("Years", fontproperties=font)
        plt.yticks([-90, -60, -30, 0, 30, 60, 90], fontproperties=font)
        plt.yticks(fontproperties=font)
        plt.xticks(fontproperties=font)
        plt.ylabel("Latitude", fontproperties=font)

        colormap = ax.pcolormesh(x_var, np.append((y_var - 2.5), 90), self.data[:, :, loop_count].T)
        cmap2 = colors.ListedColormap(['red'])
        _ = ax.pcolor(x_var, np.append((y_var - 2.5), 90), self.neg_data[:, :, loop_count].T, cmap=cmap2)

        cbar = plt.colorbar(colormap)
        cbar.ax.get_yaxis().labelpad = 2

        if self.datatype == "HNO3_mean":
            cbar.set_label("Concentration (ppb)", rotation=90, fontproperties=font)
        elif self.datatype == "H2O_mean":
            cbar.set_label("Concentration (ppm)", rotation=90, fontproperties=font)

        font_size = 16
        cbar.ax.tick_params(labelsize=font_size, width=1, length=5)

        ax.grid(which="major", axis="x", linestyle="--", color="black")

        if self.datatype == "HNO3_mean":
            plt.savefig("HNO3_pressure_at_{:.2f}_hPa".format(loop_var[loop_count]) + ".png", bbox_inches="tight")
        elif self.datatype == "H2O_mean":
            plt.savefig("H2O_pressure_at_{:.2f}_hPa".format(loop_var[loop_count]) + ".png", bbox_inches="tight")
        plt.close()

    def lat_vs_levels(self, x_var, y_var, loop_var, loop_count):
        """This is a plot of level vs lat, see docstring for lat_vs_year function for more information."""

        fig, ax = plt.subplots(1, 1, figsize=(18, 12))
        font = FontProperties()
        font.set_size(16)
        ax.set_yscale("log")
        ax.use_sticky_edges = True

        if self.datatype == "HNO3_mean":
            plt.title(r"$HNO_3$ monthly mean zonal mean at {}/{}".format(loop_var[loop_count].month,
                      loop_var[loop_count].year),
                      fontproperties=font)
        elif self.datatype == "H2O_mean":
            plt.title(r"$H_2$O monthly mean zonal mean at {}/{}".format(loop_var[loop_count].month,
                      loop_var[loop_count].year),
                      fontproperties=font)

        plt.xlabel("Latitude", fontproperties=font)
        plt.xticks([-90, -60, -30, 0, 30, 60, 90], fontproperties=font)
        plt.yticks(fontproperties=font)
        plt.xticks(fontproperties=font)
        plt.ylabel("Pressure in hPa", fontproperties=font)

        d_max = np.nanmax(self.data[loop_count, :, :])
        d_min = np.nanmin(self.data[loop_count, :, :])

        colormap = ax.pcolormesh(np.append((x_var - 2.5), 90), y_var, self.data[loop_count, :, :].T,
                                 norm=colors.LogNorm(vmin=d_min, vmax=d_max))

        cmap2 = colors.ListedColormap(['red'])
        _ = ax.pcolor(np.append((x_var - 2.5), 90), y_var, self.neg_data[loop_count, :, :].T, cmap=cmap2)

        tick_array = [0, 0.2, 0.4, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        cbar = plt.colorbar(colormap, ticks=tick_array)
        cbar.ax.set_yticklabels(tick_array)
        cbar.ax.get_yaxis().labelpad = 2

        if self.datatype == "HNO3_mean":
            cbar.set_label("Concentration (ppb)", rotation=90, fontproperties=font)
        elif self.datatype == "H2O_mean":
            cbar.set_label("Concentration (ppm)", rotation=90, fontproperties=font)
        font_size = 16
        cbar.ax.tick_params(labelsize=font_size, width=1, length=5)

        plt.gca().invert_yaxis()
        ax2 = ax.twinx()
        ax2.yaxis.set_ticks(range(0, len(y_var), 5))
        ax2.yaxis.set_label_position("right")
        ax2.tick_params(labelsize=font_size)

        if self.datatype == "HNO3_mean":
            plt.savefig("HNO3_times_{}_{}".format(loop_var[loop_count].month,
                                                  loop_var[loop_count].year) + ".png", bbox_inches="tight")
        elif self.datatype == "H2O_mean":
            plt.savefig("H2O_times_{}_{}".format(loop_var[loop_count].month,
                                                 loop_var[loop_count].year) + ".png", bbox_inches="tight")
        plt.close()

    def time_vs_levels(self, x_var, y_var, loop_var, level_idxs, loop_count):
        """This is a plot of level vs year"""

        fig, ax = plt.subplots(1, 1, figsize=(18, 12))
        font = FontProperties()
        font.set_size(16)
        ax.set_yscale("log")
        ax.use_sticky_edges = True

        if self.datatype == "HNO3_mean":
            plt.title(r"$HNO_3$ monthly mean zonal mean at latitude {:.2f}$^\circ$".format(loop_var[loop_count]),
                      fontproperties=font)
        elif self.datatype == "H2O_mean":
            plt.title(r"$H_2$O monthly mean zonal mean at latitude {:.2f}$^\circ$".format(loop_var[loop_count]),
                      fontproperties=font)

        plt.xlabel("Year", fontproperties=font)
        plt.yticks(fontproperties=font)
        plt.xticks(fontproperties=font)
        plt.ylabel("Pressure in hPa", fontproperties=font)

        d_max = np.nanmax(self.data[:, loop_count, :])
        d_min = np.nanmin(self.data[:, loop_count, :])

        colormap = ax.pcolormesh(x_var, y_var, self.data[:, loop_count, :].T,
                                 norm=colors.LogNorm(vmin=d_min, vmax=d_max))
        cmap2 = colors.ListedColormap(['red'])
        _ = ax.pcolor(x_var, y_var, self.neg_data[:, loop_count, :].T, cmap=cmap2)

        tick_array = [0, 0.2, 0.4, 0.6, 0.8, 1, 1.5, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        cbar = plt.colorbar(colormap, ticks=tick_array)
        cbar.ax.set_yticklabels(tick_array)
        cbar.ax.get_yaxis().labelpad = 2

        if self.datatype == "HNO3_mean":
            cbar.set_label("Concentration (ppb)", rotation=90, fontproperties=font)
        elif self.datatype == "H2O_mean":
            cbar.set_label("Concentration (ppm)", rotation=90, fontproperties=font)

        font_size = 16
        cbar.ax.tick_params(labelsize=font_size, width=1, length=5)
        for label in cbar.ax.yaxis.get_ticklabels()[::2]:
            label.set_visible(False)

        ax.grid(which="both", axis="x", linestyle="--", color="black")

        min_level = np.nanmin(level_idxs)
        max_level = np.nanmax(level_idxs)

        plt.gca().invert_yaxis()
        ax2 = ax.twinx()
        ax2.set_ylim(min_level, max_level)
        ax2.yaxis.set_ticks(range(min_level, max_level, 5))
        ax2.yaxis.set_label_position("right")
        ax2.tick_params(labelsize=font_size)

        if self.datatype == "HN03_mean":
            plt.savefig("HNO3_pressures_{:.2f}".format(loop_var[loop_count]) + "hPa.png", bbox_inches="tight")
        elif self.datatype == "H2O_mean":
            plt.savefig("H2O_pressures_{:.2f}".format(loop_var[loop_count]) + "hPa.png", bbox_inches="tight")
        plt.close()


def plot_looper(loop_var, data_var, x_var, y_var, ppm_or_ppb, filepath, level_idxs_y_n):
    """This is where the plots are looped over. The amount of times this occurs is determined by
       the unused variable out of x_var and y_var choosing from time, level or lat."""

    fh = Dataset(filepath, mode="r")

    data_var = (data_var.split('_'))[0]+'_mean'
    # the above line is used to convert the data arg to the correctly formatted string for iteration in the plots.

    datatype = data_var

    if level_idxs_y_n is True:
        level_idxs_out = fh.variables["level_idxs"][:]
    else:
        level_idxs_out = None
    # Used to access the level_idxs variable in the netCDF file, if the plot calls for its use.

    if x_var == "time":
        x_var_out = nc.num2date(fh.variables["time"][:], fh.variables['time'].units)
    else:
        x_var_out = fh.variables[x_var][:]
    # Used to call the time variable from the netCDF file and uses nc.num2date from the netCDF4 module to convert it
    # to a plottable format.

    if loop_var == "time":
        loop_var_out = nc.num2date(fh.variables["time"][:], fh.variables['time'].units)
    else:
        loop_var_out = fh.variables[loop_var][:]

    y_var_out = fh.variables[y_var][:]
    plot_type = plot_decider(x_var, y_var)

    if ppm_or_ppb == "ppm":
        data = fh.variables[data_var][:].filled(np.nan) * 1e6
    else:
        data = fh.variables[data_var][:].filled(np.nan) * 1e9

    neg_data = np.full_like(data, np.nan)
    mask = data < 0
    neg_data[mask] = data[mask]
    data[mask] = np.nan

    outplot = Plot(data, neg_data, datatype)

    if plot_type == "lat_vs_year":
        for idx in range(len(loop_var_out)):
            if not np.isnan(data[:, :, idx]).all():
                outplot.lat_vs_year(x_var_out, y_var_out, loop_var_out, idx)

    elif plot_type == "lat_vs_levels":
        for idx in range(len(loop_var_out)):
            if not np.isnan(data[idx, :, :]).all():
                outplot.lat_vs_levels(x_var_out, y_var_out, loop_var_out, idx)

    elif plot_type == "time_vs_levels":
        for idx in range(len(loop_var_out)):
            if not np.isnan(data[:, idx, :]).all():
                outplot.time_vs_levels(x_var_out, y_var_out, loop_var_out, level_idxs_out, idx)


def plot_decider(x_var, y_var):
    """takes the output from the user_input_specs function and decides which plot under the Plot class
       should be called, based on the input x_var and y_var arguments."""

    if x_var == "time" and y_var == "latitude":
        return "lat_vs_year"
    elif x_var == "latitude" and y_var == "level":
        return "lat_vs_levels"
    elif x_var == "time" and y_var == "level":
        return "time_vs_levels"


def args_in():
    """x_var and y_var accepts inputs as either 'time', 'level', or 'latitude'"""

    parser = argparse.ArgumentParser(
        description="Plots user-specified graphs via input variables and netCDF files.")
    parser.add_argument('--variable',
                        default='H2O_vmr',
                        choices=['O3_vmr', 'O3_nd', 'H2O_vmr', 'HNO3_vmr'],
                        help="Variables to be extracted")
    parser.add_argument("--filepath", type=str, help="Wrong filepath, input filepath to netCDF file.")
    parser.add_argument("--x-var", type=str, help="Wrong arg for x_variable, see choices.",
                        choices=["time", "latitude"], default='time')
    parser.add_argument("--y-var", type=str, help="Wrong arg for y_variable, see choices.",
                        choices=["latitude", "level"], default='latitude')

    args = parser.parse_args()

    return args


def main():
    """This is where all the user input is gained from, in order to get the data and variable information."""

    args = args_in()
    # imports the runtime args from the args_in function

    filepath = args.filepath
    # location of the netCDF file.
    variable = args.variable
    # specifies which data variable is selected from the netCDF file, which later
    # becomes an attribute of the Plot class.
    database_type = ProfileCollection.CORRECTED
    # this is used in the filepath as part of the bdbp.collection module.
    x_var = args.x_var
    y_var = args.y_var
    vert = 'press'

    if filepath is None:
        filepath = default_mmzm_path(args.variable, database_type)
    input_fname = create_mmzm_fname(database_type, None, filepath, variable, vert)

    if variable == "HNO3":
        ppm_or_ppb = "ppb"
    else:
        ppm_or_ppb = "ppm"
    # This is used to infer how the concentration of the data should be scaled. ppm = * le6, ppb = * le9.

    if x_var == "time" and y_var == "level":
        level_idxs_y_n = True
    else:
        level_idxs_y_n = False
    # Used to inver whether the level_idxs variable will be needed for the plot.

    if x_var == "time" and y_var == "latitude":
        loop_var = "level"
    elif x_var == "latitude" and y_var == "level":
        loop_var = "time"
    else:
        loop_var = "latitude"
    # Used to infer which variable will be used for the loop count and iteration, as whichever variable is not
    # being plotted as one of the axes must be the loop_var.

    plot_looper(loop_var, variable, x_var, y_var, ppm_or_ppb, input_fname, level_idxs_y_n)
    # The file locations of two of the previously used netCDF files are below:
    # HNO3_file = "/mnt/temp/BSVertNitricAcid/version_3.0.1.0/Released/BSVerticalNitricAcid_MR_PRS_Tier0.0_v.1.0.nc"
    # H2O_file = "/mnt/temp/BSVertWater/version_3.0.1.0/Released/BSVerticalWater_MR_PRS_Tier0.0_v.1.0.nc"


main()
