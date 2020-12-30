"""
+ ========================================================================================== +
+    Author: Johnny (Shaun) Lowis, for Bodeker Scientific.                                   +
+    Script for modelling approximate pm output of household fires, based off of             +
+    a household's fire lighting behaviour and type of woodburner.                           +
+ ========================================================================================== +

A quick rundown of the bottom up model:
The goal of the model is to produce an emissions map of a city. This is done by inputting a temperature
for the entire city and a time of day. This time will be in 24H time to simplify things. The idea is then
to create a normal distribution of this city-wide temperature. Each household in the city has a certain time
and a certain temperature at which they usually light their woodburners. This time and temperature for each
household is then compared to the city-wide temperature. This is done by way of plotting a normal distribution
for each household's usual fire-lighting temperature. The normal distribution of the city-wide temperature is
compared with each household's usual fire-lighting temperature's normal distribution. If they overlap enough,
the household is likely to light their fire at that time. This fire, for simplicity, burns until 22:00.
Because of this comparison, some households will light their fires at the inputted city-wide temperature and
time and others won't. These households' woodburners will then produce PM2.5 emissions when burning. These are
modelled using the Woodburner class in the bottom_up_model. These households are then each associated with
a meshblock. By meshblock, it is meant that the city is divided into a grid of n cells. As such, the physical
location of each household will be part of some meshblock. These individual household emissions modelled by the
Woodburner class are then aggregated for the entire meshblock. The amount of emissions per meshblock then
determines the colour of the meshblock. The final product is then an emissions map of differently coloured
meshblocks for a given time and temperature.

Confluence documentation for this model:
https://confluence.bodekerscientific.com/pages/resumedraft.action?draftId=42467528&draftShareId=6e6a9c57-cca4-4cfb-b007-ab36c2fe693a&

The ASCII pic can be found at
https://asciiart.website/index.php?art=movies/star%20wars

"""

import os
import csv
import geopandas as gpd
import contextily as ctx
import xarray as xr
import random as rand
import numpy as np
import pandas as pd
import scipy.integrate as spi
from scipy import stats as ss
from matplotlib import pyplot as plt

timestep_emissions_dict = {}


class Control:
    """ The Control class is designed to provide a 'wrapper' of sorts for all of the subclasses in the model.
        as such, the user initialises the Control class with some data, then the Control class generates
        the appropriate amount of Meshblock, Household and Woodburner objects as specified by the user.
        This specification then is done when calling the Control class. The idea being to structure
        the syntax such that a model run can be done using:

        model_run = Control(global_temp, temp_sigma, time, city_data, outdir)
        model_run.run(**kwargs)

        These attributes will be expanded on in the documentation for __init__ below:
    """

    def __init__(self, city_data, weather_data, outdir):
        """ Here is where the above mentioned user inputs are defined. As such, when calling Control, the user
            provides the runtime conditions for the entire bottom up model.
        :param city_data: This should be a .csv file, an example of which can be generated using gen_city_data()
        :param weather_data: This should be a .csv file, an example of which can be generated using gen_city_data()
        :param outdir: The output directory of the model's changed .csv file. This is where the template .csv file
                       and README.txt can be written to optionally.

                                                -Placeholder attributes-
        :temp_sigma: The variance in the city-wide temperature. This is used to calculate a gaussian function
                     for the city-wide temperature. This gaussian is then compared to the Household objects'
                     gaussians. The overlap between these two functions then determine whether the respective
        """
        self.city_data = city_data
        self.weather_data = weather_data
        self.outdir = outdir

        self.temp_sigma = 2

    def run(self, timesteps, show_plots=False):
        """ The user has to call the run inner function of the Control class to actually perform a model run.
            This allows more control over running the model as the user can create a Control class object with the
            necessary inputs, then perform a model run using the parameters specified when initially calling the
            Control class.
        :param timesteps: How many time steps of the weather_data.csv file the model should run for.
        wood burner emissions functions and the city-wide and household Gaussian distributions.
                                                -**kwargs-
        :param show_plots: Whether or not to visualise the subclasses of the model. This means plotting ALL of the
                           visualisation of ALL of the Households, Woodburners. This is only recommended for small
                           runs and/or debugging the model.
        """
        # __init__(self, city_temp, temp_sigma, time, city_data, outdir, show_plots=False)
        # Reading in the city_data and weather_data files as pandas dataframes:
        city_data = pd.read_csv(self.city_data)
        weather_data = pd.read_csv(self.weather_data)
        # Empty list of emissions for appending to later
        emissions = []

        # Looping through the city_data dataframe and initializing each meshblock object.
        # Each meshblock appends an emission value to the emissions array. Since the order corresponds to city_data
        # this can be directly appended to the output file later as a new column of emissions.
        for i in range(len(city_data)):
            if show_plots:
                meshblock = Meshblock(weather_data['air_temperature'][0:timesteps], weather_data['time'][0:timesteps],
                                      city_data[:].iloc[i], self.outdir, timesteps, True)
                emissions.append(meshblock.emissions)
                print(f'Finished simulating meshblock {i}...')
            else:
                meshblock = Meshblock(weather_data['air_temperature'][0:timesteps], weather_data['time'][0:timesteps],
                                      city_data[:].iloc[i], self.outdir, timesteps, False)
                emissions.append(meshblock.emissions)
                print(f'Finished simulating meshblock {i}...')

        # Creating a copy of city_data.csv and appending the emissions of each meshblock to it as a new column.
        out_path = os.path.join(self.outdir, 'city_emissions.csv')
        city_data.to_csv(out_path, index=False)
        emissions_data = pd.read_csv(out_path)
        emissions_data['emissions'] = emissions
        emissions_data.to_csv(out_path, index=False)

        path = os.path.join(self.outdir, 'hourly_emissions.csv')
        with open(path, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['times', 'emissions'])
            sorted_keys = sorted(timestep_emissions_dict)

            for key in sorted_keys:
                value = timestep_emissions_dict[key]
                writer.writerow([key, value]) 

        timesteps = sorted(timestep_emissions_dict.keys())
        userin = ""
        # Create an input loop that prompts the user:
        while userin is not "END":
            userin = input('You can query the model for emissions over a time period like: start_time-end_time where \n'
                           'these times should be in datetime format, i.e: 2019-05-31 13:00:00 \n'
                          f'The time steps the model was run for are: {timesteps}\n'
                           'End this prompt by "END"\n'
                           'Enter the start time please: ')

            if userin == "END":
                break
            else:
                pass

            emissions = 0
            # Indexing the start and end time from the user input:
            start_time = userin
            end_time = input('End this prompt by "END"\n'
                             'Enter the end time please: ')

            if end_time == "END":
                break
            else:
                pass

            try:
                times = pd.date_range(start_time, end_time, freq='H').tolist()
                py_dates = []

                for time in times:
                    time = time.to_pydatetime()
                    py_dates.append(str(time))

                for time in py_dates:
                    try:
                        emissions += timestep_emissions_dict[time]
                    except:
                        pass

            except ValueError:
                print('Invalid input')
                pass

            print(f'The emissions between {start_time} and {end_time} are: {emissions}')

            userin = input('Any other time periods? End this prompt by "END".\n'
                           'Input here (Y/N): ')
            if userin == "END" or userin == "N":
                break
            else:
                pass

        print("\n"
            "   _________________________________ \n"
            "  |:::::::::::::;;::::::::::::::::::|\n"
            "  |:::::::::::'~||~~~``:::::::::::::|\n"
            "  |::::::::'   .':     o`:::::::::::|\n"
            "  |:::::::' oo | |o  o    ::::::::::|\n"
            "  |::::::: 8  .'.'    8 o  :::::::::|\n"
            "  |::::::: 8  | |     8    :::::::::|\n"
            "  |::::::: _._| |_,...8    :::::::::|\n"
            "  |::::::'~--.   .--. `.   `::::::::|\n"
            "  |:::::'     =8     ~  \ o ::::::::|\n"
            "  |::::'       8._ 88.   \ o::::::::|\n"
            "  |:::'   __. ,.ooo~~.    \ o`::::::|\n"
            "  |:::   . -. 88`78o/:     \  `:::::|\n"
            "  |::'     /. o o \ ::      \88`::::|   You have done well.\n"
            "  |:;     o|| 8 8 |d.        `8 `:::|   Model run complete.\n"
            "  |:.       - ^ ^ -'           `-`::|\n"
            "  |::.                          .:::|\n"
            "  |:::::.....           ::'     ``::|\n"
            "  |::::::::-'`-        88          `|\n\n"

              f'Files outputted here: {self.outdir}')


class Meshblock:
    """ The meshblock is given a brief explanation in the program docstring at the top of this script as:
        'By meshblock, it is meant that the city is divided into a grid of n cells. As such, the physical
        location of each household will be part of some meshblock. These individual household emissions modelled by the
        Woodburner class are then aggregated for the entire meshblock. The amount of emissions per meshblock then
        determines the colour of the meshblock. The final product is then an emissions map of differently coloured
        meshblocks for a given time and temperature.'

        Here each meshblock object has n amount of households associated with it. Each household then, has a woodburner
        associated with it. Hence the model has a top-down structure: Meshblock --> Households --> Woodburner.
        Then when the emissions from the woodburner is calculated per household, the households' emissions are then
        aggregated for each meshblock. All of the meshblocks then form a city-wide grid. As such this process is
        repeated for each meshblock in the city-wide grid.
    """

    def __init__(self, city_temp, time, city_data, outdir, timesteps, show_plots=False):
        """ The Meshblock class is a child of the Control class. This is because it shares all attributes bar
            show_plots with Control. An amount of Meshblock objects will be produced when """
        self.city_temp = city_temp
        self.time = time
        self.city_data = city_data
        self.outdir = outdir
        self.timesteps = timesteps
        self.show_plots = show_plots

        self.temp_sigma = 2
        self.total_emissions = 0
        self.emissions = self._run()

    def _run(self):
        """ This function generates the amount of Household objects specified by the city_data.csv input file.
            For now, there are only three different types of wood burners used in this model. As such, the city_data
            file splits up the amount of Households into one of the three wood burner types. These get initialised here
            and their emissions functions are calculated in the Household class and aggregated to the total emissions
            of the overall meshblock here. Finally, the total emissions are appended to the city_data.csv file as a
            new column titled 'emissions'.
        """

        # Generating the Household objects and calculating their emissions values.
        # Here the total number of Households with type 1 wood burners are generated:
        for i in range(0, int(self.city_data['num_woodburner1'])):
            if self.show_plots:
                household_1 = Household(self.time, self.city_temp, 1, self.city_data['area'], True)
                self.total_emissions += household_1.emissions
            else:
                household_1 = Household(self.time, self.city_temp,  1, self.city_data['area'])
                self.total_emissions += household_1.emissions

        # Here the total number of Households with type 2 wood burners are generated:
        for i in range(0, int(self.city_data['num_woodburner2'])):
            if self.show_plots:
                household_2 = Household(self.time, self.city_temp, 2, self.city_data['area'], True)
                self.total_emissions += household_2.emissions
            else:
                household_2 = Household(self.time, self.city_temp, 2, self.city_data['area'])
                self.total_emissions += household_2.emissions

        # Here the total number of Households with type 3 wood burners are generated:
        for i in range(0, int(self.city_data['num_woodburner3'])):
            if self.show_plots:
                household_3 = Household(self.time, self.city_temp, 3, self.city_data['area'], True)
                self.total_emissions += household_3.emissions
            else:
                household_3 = Household(self.time, self.city_temp, 3, self.city_data['area'])
                self.total_emissions += household_3.emissions

        # Can scale to emissions per m^2 by doing self.total_emissions / self.city_data['area']
        scaled_emissions = self.total_emissions / self.city_data['area']

        return scaled_emissions


class Household:
    """ In this definition of a household, the household object will be comprised of x amount of people living
        together and sharing a wood burner in their home. Due to the assumption that there is a singular
        wood burner per household, the fire lighting behaviour of all of the people in a home can be viewed as a single
        household's behaviour. Here fire lighting behaviour refers to the usual temperature that the household
        lights their fire, as well as the usual time at which the household lights their fire. This follows
        as most living units will light their wood burners or turn on their heating at some cold temperature/time.
        This temperature/time may vary throughout the year, or day or some length of time. As such, this variance is
        then the difference between the usual, average, time/temperature at which the household lights its fire and
        some outlying time/temperature at which the fire was lit. This then, is how real-world information about
        households' fire lighting behaviour can be gathered and then inputted into this model and used to generate
        an emissions map.
    """

    def __init__(self, time, global_temp, woodburner_type, area, show_plots=False):
        """ Initialise class attributes.
        :param time: The range of time steps for which the model is run.
        :param global_temp: The city-wide temperatures for which the model is run.
                                    -- **kwargs --
        :param show_plots: Whether to show the plots of the city-wide and household temperature gaussians as well
                           as whether or not to show the emissions function of the Woodburner."""
        self.time = time
        self.global_temp = global_temp
        self.woodburner_type = woodburner_type
        self.area = area
        self.show_plots = show_plots

        self._gen_household_data()

        if self.show_plots:
            self._gaussians()

        self.emissions = self._compare()

    def _gen_household_data(self):
        """ This function will generate the Household's average time and average temperature that they light
            their fires at. This will be done randomly for each household. The current ranges that the data will be
            generated in is between 16:00 and 21:00 for the fire start time and between 0 and 8 degrees celsius.
            The time and temperature variance will be between 1 and 3 hours and degrees celsius respectively.
            This data may be based off of real behaviour after feedback from ECAN or census data in the future.
        """
        # mu_time refers to the average time that the household will usually light their fire.
        self.mu_time = rand.randrange(16, 23)
        self.h_time_sigma = rand.randrange(1, 3)
        self.mu_temp = rand.randrange(0, 8)

        # Here follows some print statements used during debugging:
        # print(f'Household mu_time value is: {self.mu_time}')
        # print(f'Household h_time_sigma value is: {self.h_time_sigma}')
        # print(f'Household mu_temp value is: {self.mu_temp}')
        # print(f'Household h_temp_sigma value is: {self.h_temp_sigma}')

    def _gaussians(self):
        """ Here follows the code for visualising the city-wide and household temperature gaussians.
            to make comparing them easier, their integrals are also plotted, as comparing the overlap
            is simpler by computing the y-value (the probability) at a certain x-value (a certain time)
            of both integrals, and directly comparing the probabilities than trying to find the area of
            the overlapping gaussians and comparing that overlap.
        """
        # This is a list derived from ECAN's census data around emissions in Christchurch. There are 24
        # elements in the temp_dist array. Each element represents an hour in a day. The value of the element
        # at that hour is the percentage of the total emissions for the day.
        self.temp_dist = [1.9, 1.3, 1.0, 0.9, 0.8, 0.7, 0.6, 3.7, 3.7, 2.1, 2.1, 2.7, 2.9, 3.2, 3.5, 4.2,
                          4.5, 5.3, 7.6, 11.5, 11.8, 10.8, 9.7, 3.3]

        # Plotting the household gaussians:
        plot_gaussians(self.time, self.h_time_sigma, self.time, showplots=True)
        # Plotting the city-wide gaussians:
        plot_gaussians(self.mu_time, self.h_time_sigma, self.time, showplots=True)

    def _compare(self):
        """ This function is used to make the comparison between the Household object's temperature gaussian
            and the city-wide temperature's gaussian. Both of these gaussian distributions can be plotted with
            the above _gaussians function if needed. The comparison being made in this function is whether or not
            the difference between the gaussian integrals of the two temperatures is less than a random decimal
            number between zero and one. If this is true, then the Household that is being compared will light
            their fire at the global time defined in the Control class. This process can then be repeated,
            running the model at several global time values. Currently the model assumes that Household objects
            will have the same temperature gaussian, where the global temperature and the global time of the model
            changes with each run, therefore resulting in different Household objects starting their respective
            fires and thereby initialising their respective Woodburner objects at different times during the day.
        """
        emissions = 0
        # The following section code makes a list, temp_integrals, of dictionaries for each temperature between
        # 0 and 9 degrees Celsius. In the dictionary for each temperature, a gaussian integral is evaluated for
        # each hour between 15:00 and 21:00.

        for i in range(len(self.time) - 1):
            curr_timestep = int(self.time[i][11:13])
            curr_temp = self.global_temp[i]

            temp_integrals = []
            for temp in range(0, 9):
                times = [15, 16, 17, 17, 18, 19, 20, 21]
                temp_dict = {}

                for time in times:
                    household_integral = ss.norm.cdf(time, self.mu_time, self.h_time_sigma ** 2)
                    temp_dict[time] = household_integral

                self.h_time_sigma -= 0.05
                temp_integrals.append(temp_dict)

            # Generating a random number between 0 and 1. If the household's integral at the current time
            # or at the current temperature is bigger than the compval, then the current time and temperature
            # is the starting time and temperature of the woodburner.

            compval = rand.uniform(0, 1)

            # If the temperature is higher than 9 degrees, no fires get started. This can be changed later on.
            if np.round(curr_temp) > 9:
                continue

            # This catch is for hours outside of the 15:00 to 22:00 range, where no fires are lit.
            try:
                curr_temp_dict = temp_integrals[int(np.round(curr_temp))]
            except IndexError:
                continue

            # If the compval is less than the integral evaluated for the current time and date, or if the current
            # temperature is less than 0 degrees, fires should be lit.

            # burner_type, start_time=None, show_plots=False
            try:
                if compval < curr_temp_dict[curr_timestep] or np.round(curr_temp) < 0:
                    start_time = self.time[i]

                    if self.show_plots:
                        woodburner = Woodburner(self.woodburner_type, start_time=start_time, show_plots=True)
                    
                    else:
                        woodburner = Woodburner(self.woodburner_type, start_time=start_time)

                    emissions += woodburner.pollute(start_time, self.time[i + 1])
                    dict_emissions = emissions / self.area

                    try:
                        dict_emissions += timestep_emissions_dict[str(i)]
                    except KeyError:
                        pass

                # If the compval is not less than the household integral, then the fire doesn't start,
                # therefore no emissions:
                else:
                    emissions += 0
            except KeyError:
                pass

        return emissions


class Woodburner:
    """ This class is used to model the emissions output of a lit woodburner. By emissions, specifically PM2.5
        emissions. To model the emissions of a woodburner, the class generates a piecewise function of emissions
        over time. The emissions are on the y-axis and time the x-axis. To help model this emissions function,
        I have broken the function up into its components. The stages of combustion during wood burning consists
        of the wood being lit on fire, the volatiles in the wood evaporating and causing incomplete combustion,
        therefore more emissions. After a peak emission is reached, the emissions reduce to a steady burn stage.
        This is where the 'cleanest' burn, or least amount of emissions occur as the carbon reacts with oxygen.
        For the purposes of the model, we are assuming that manual dampening takes place. This causes the fire
        to enter an oxygen scarce state, from steady burn, as such much more emissions occur. After the emissions
        peak, the fire is starved of oxygen and starts to die out. During this smoldering phase, the emissions
        approach 0, at which point the fire dies.
        The emissions profile of wood burning will roughly follow the shape below:
        Given below is a diagram of the function, where y-axis = g/kg emissions, x-axis = Time.
        Annotations explain the different segments of the piecewise function, where [variable] refers to the
        attributes declared below in the Woodburner's __init__ section.

        |                              E           Where: A = The start of the fire. [start_time]
        |           B                 /\                  B = Maximum pm output of the evaporation stage. [max_evap]
        |          /\                / \                  C = Start of the steady burn stage. [steady_pm]
        |         / \               /  \                  D = Start of manual dampening of the fire. [steady_pm]
        |        /  \C____________D/   \                  E = Peak emissions of the dampening period. [max_damp]
        |       /                      \                  F = The fire dies out here, no more emissions. [smolder]
        |_____A/_______________________\F_______          G -> H = Time from A to B [evap]
            G|   H| I|            J|  K|L|                H -> I = Time from B to C [evap_steady]
                                                          I -> J = Time from C to D [steady]
                                                          J -> K = Time from D to E [damp_time]
                                                          K -> L = Time from E to F [smolder]

        Note: C=D as these have the same g/kg emissions value. Therefore it is simpler to equate these two.
    """

    def __init__(self, burner_type, start_time=None, show_plots=False):
        """ Initialise class attributes. Only start_time and type needs to be inputted to the Woodburner object as the
            rest of the objects' attributes are generated using _gen_template_woodburners()
        :param start_time: The start time of the fire, calculated by the Household class.
        :param timestep: If the fire has already been startedm then this timestep is the current time of the model.
        :param burner_type: Which of the 3 template wood burners should be passed to.
        :param start_time: Start time of the fire. [time]
                                                    -- **kwargs --
        :param show_plots: Boolean; whether or not to plot the emissions function of the woodburner.

                                                    -- Secondary attributes --
        These are initialised here as these attributes will be given values according to which template the
        _gen_template_woodburners() function gets passed. These are effectively placeholders initialised here.

        evap: (Evap = evaporation), Time taken of the moisture loss stage. [time]
        max_evap: Maximum pm produced during evaporation phase of combustion. [pm]
        evap_steady: Time taken to transition from maximum pm phase to steady burning. [time]
        steady: How long steady burn lasts. [time]
        steady_pm: Amount of pm produced at steady burn state. [pm]
        max_damp: Manual dampening of fire cause second maximum pm produced. [pm]
        damp_time: Time taken from start of dampening to second max pm. [time]
        smolder: Time taken for fire to die out. [time]
        emissions: This is used to calculate the emissions value and store it as an attribute of the wood burner.
        """
        self.start_time = start_time
        self.burner_type = burner_type
        self.plot_fire = show_plots

        # Setting the placeholder attributes equal to zero for later use:
        self.evap = 0
        self.max_evap = 0
        self.evap_steady = 0
        self.steady = 0
        self.steady_pm = 0
        self.max_damp = 0
        self.damp_time = 0
        self.smolder = 0
        self.emissions = 0
        self.start_hour = self.start_time[11:13]

        self._gen_template_woodburners(burner_type=burner_type)

        if self.plot_fire:
            self._plot_fire()

    def _equations(self):
        """ Here the equations for the 'parts' of the piecewise function is calculated. This is done using the
            parabola_maker function, as we know the x and y coordinates of the parabola's peak and the shape we want
            from the attributes we defined in __init__. As such, this function uses numpy's linspace function
            to fill the datavalues from the start of one 'piece' of the function, to the end, i.e. from start_time
            to the evap stage. Then parabola_maker produces an equation for that 'piece' of the function. This process
            is then repeated for all of the 'pieces' of the function, until an entire piecewise function is plotted.
        :return: The x_domains from the linspace function, and the equations of the parabola 'pieces'.
        """

        # Declare the domains for fire's piecewise function
        # These are the x ranges of the pieces of the emissions function.
        x1 = np.linspace(self.start_hour, self.evap)
        x2 = np.linspace(self.evap, self.evap_steady)
        x3 = np.linspace(self.evap_steady, self.steady)
        x4 = np.linspace(self.steady, self.damp_time)
        x5 = np.linspace(self.damp_time, self.smolder)

        # Making the functions that are part of the piecewise function. These are modelled as parabolas.
        # Arguments of parabola_maker function: parabola_maker(y_point, x_point, h_vert, k_vert, x)
        y = parabola_maker(0, self.start_hour, self.evap, self.max_evap, x1)
        g = parabola_maker(self.steady_pm, self.evap_steady, self.evap, self.max_evap, x2)
        h = x3 / x3 + (self.steady_pm - 1)
        j = parabola_maker(self.steady_pm, self.steady, self.damp_time, self.max_damp, x4)
        k = parabola_maker(0, self.smolder, self.damp_time, self.max_damp, x5)

        return x1, x2, x3, x4, x5, y, g, h, j, k

    def _gen_template_woodburners(self, burner_type):
        """ Here I aim to make some template Woodburners. By this I will call 3 Woodburner objects with their
            emissions functions. To make these emissions functions as accurate as possible, I have made a list of
            the most commonly used types of wood, along with their emissions, sourced from:
            https://www.consumer.org.nz/articles/woodburner-emissions#article-test-results

            The idea behind making a small amount of template Woodburner objects, is that I can then more accurately
            create their emissions functions, in that I can base these emissions functions off of real-world wood
            burners and data, rather than randomly generating emissions functions, which would be inaccurate.

            These results were obtained using the method outlined on page 2, section 1.1 of the following report:
            https://www.environet.co.nz/environet/documents/In_home_testing_of_particulate_emissions_from_NES_authorised_woodburners.pdf

            Here, I have for simplicity assumed that one 'piece' of wood is approximately one kilogram.
            Since the Woodburner objects will have different emissions functions depending on what time of the day
            the fire is started, the start_time attribute of the respective Woodburner objects will be subject to
            the comparison made between the Household object's temperature gaussian and the city-wide temperature
            gaussian as explained in the program docstring.

            Here is the table of wood types along with their emissions values, sourced from Consumer.org [link above]
            Wood type[sort] 	    Fuel load (pieces) 	Burn time (minutes)[sort] 	Emissions (g/kg)[sort;asc]
            Seasoned radiata 	             4 	                   169 	                    2.6
            Seasoned macrocarpa 	         4                     148 	                    1.9
            Seasoned blue gum 	             4                     173 	                    1.6
            Seasoned manuka 	             6                     154                     	4.4
            Unseasoned radiata 	             4 	                   105                     	12
            Seasoned radiata                 2 	                   175                     	24
            Seasoned blue gum                2                     194 	                    4.2

            In order to use the above values in my emissions function, the comparison function must determine a start
            time for the Household object. Once this start time is found, We are assuming that the Woodburner gets
            extinguished at 10pm. For simplicity's sake, this will be considered the termination point of the Woodburner
            object's emission's function (in the Woodburner class docstring, this will be the point labelled F on the
            plot I made). To calculate how much wood must be burned, we then take 22 - start time of the fire, where
            22 is 10pm in 24-hour time. Since we know the burn time per piece for each wood type from above, we can
            then calculate how much wood needs to be burned in order to achieve the burn time found above.

            I have found a list of ECAN approved wood burners on the following website:
            https://www.ecan.govt.nz/data/authorised-burners/

            Perhaps in later iterations of this model any wood burner on the above website can be used to generate
            an emissions function, but for now I will manually make 3 'template' wood burners. Since these will have
            as similar emissions profiles to the real world wood burners in the above website as possible, they should
            provide a better basis for an accurate emissions map as a final product.

                                                    **kwargs
            Here I have decided to make the first template wood burner using seasoned radiata wood, and the emissions
            of the Froling S4 turbo 15 wood burner, made by C H Faul & Company Limited.
            The second template uses seasoned macrocarpa and the emissions of the Bionic Fire Studio wood burner,
            made by EnviroSolve Limited.
            The third template uses seasoned blue gum and the emissions of the Masport Cromwell wood burner made by
            Glen Dimplex New Zealand Limited.
            All of these wood burners have been sourced from ECAN's list of approved wood burners, as linked above.
        """
        self.start_hour = int(self.start_hour)
        end_time = 22
        # Burn time in hours:
        if end_time > self.start_hour:
            burn_time_h = end_time - self.start_hour
            burn_time_m = burn_time_h * 60
        else:
            burn_time_h = 0
            # Burn time in minutes:
            burn_time_m = burn_time_h * 60

        if burner_type == 1:
            # Here I define the emissions values in g/kg as given on the ECAN website cited above.
            # The 0.31 is from the emission factor of the woodburner on the ECAN website, the 2.6 is from the wood type.
            # For now the values used to determine the emissions are arbitrary, i.e. that the max_evap is twice the
            # pm emissions of the steady pm value.
            woodburner_emission_factor = 0.31
            wood_emission_factor = 2.6
            # Calculating how much wood in kg is burnt over the course of the fire being lit
            burnt_wood_weight = burn_time_m / 169
            # Adding the emissions from the wood to the fire
            self.emissions += burnt_wood_weight * wood_emission_factor
            # Print statement for debugging:
            # print(f'Emissions of the wood from burner type 1 is: {burnt_wood_weight * wood_emission_factor}')
            self.steady_pm = woodburner_emission_factor

        if burner_type == 2:
            # The second template uses seasoned macrocarpa and the emissions of the Bionic Fire Studio wood burner,
            # made by EnviroSolve Limited.
            woodburner_emission_factor = 0.5
            wood_emission_factor = 1.9
            # Calculating how much wood in kg is burnt over the course of the fire being lit
            burnt_wood_weight = burn_time_m / 148
            # Adding the emissions from the wood to the fire
            self.emissions += burnt_wood_weight * wood_emission_factor
            # Print statement for debugging:
            # print(f'Emissions of the wood from burner type 2 is: {burnt_wood_weight * wood_emission_factor}')
            self.steady_pm = woodburner_emission_factor

        if burner_type == 3:
            # The third template uses seasoned blue gum and the emissions of the Masport Cromwell wood burner made by
            # Glen Dimplex New Zealand Limited.
            woodburner_emission_factor = 0.47
            wood_emission_factor = 1.6
            # Calculating how much wood in kg is burnt over the course of the fire being lit
            burnt_wood_weight = burn_time_m / 173
            # Adding the emissions from the wood to the fire
            self.emissions += burnt_wood_weight * wood_emission_factor
            # Print statement for debugging:
            # print(f'Emissions of the wood from burner type 3 is: {burnt_wood_weight * wood_emission_factor}')
            self.steady_pm = woodburner_emission_factor

        self.max_evap = self.steady_pm * 3
        self.max_damp = self.steady_pm * 4
        self.evap = ((1 / 6) * burn_time_h) + self.start_hour
        self.evap_steady = ((1 / 6) * burn_time_h) + self.evap
        self.steady = ((2 / 6) * burn_time_h) + self.evap_steady
        self.damp_time = ((1 / 6) * burn_time_h) + self.steady
        self.smolder = ((1 / 6) * burn_time_h) + self.damp_time

    def _plot_fire(self):
        """ Calls the _equations function to calculate the x domains and the equations of the line, of the piecewise
            function. These are then used to render a combined plot of all of the lines. Each Woodburner object has
            its own piecewise function, determined by the attributes declared and explained in __init__().
        :return: Combined plot of the piecewise function.
        """
        # Import domains and parabolic equations from _equations()
        x1, x2, x3, x4, x5, y, g, h, j, k = self._equations()

        # Initialise Figure object for plotting
        fig = plt.figure()

        axes = [y, g, h, j, k]
        labels = ["Peak PM, evaporation", "Evaporation to steady state burn", "Steady burn",
                  "Manual dampening", "Peak PM dampening, time to fire smothers"]
        xvals = [x1, x2, x3, x4, x5]

        # Plot component functions of piecewise function
        for num, i in enumerate(axes):
            plot_axis(fig, axes[num - 1], xvals[num - 1], labels[num - 1])

        # Add in plot descriptors
        fig.suptitle("Burner type {}, pm emissions vs time of wood burning".format(self.burner_type))
        plt.ylabel("emissions in g/kg")
        plt.xlabel("Time in hours from 0 to 24")

        plt.xticks(np.arange(self.start_hour, 25))

        # Show the plot, can add in functionality for saving plot.
        plt.show()

    def pollute(self, start_time, end_time):
        """ Calculates the areas of sections of the Woodburner's emissions function.
            This is done using scipy's trapz function
        :return: 2-D array of emissions at a certain time
        """
        x1, x2, x3, x4, x5, y, g, h, j, k = self._equations()

        xvals = [self.start_hour, self.evap, self.evap_steady, self.steady, self.damp_time]

        for n, val in enumerate(xvals):
            try:
                if (float(self.start_hour) >= val) & (float(self.start_hour) <= xvals[n + 1]):
                    l_bound = val
                    u_bound = xvals[n+1]
                else:
                    pass
            except IndexError:
                break
        
        funcs_dict = {xvals[0]: y, xvals[1]: g, xvals[2]: h, xvals[3]: j, xvals[4]:k}

        area = spi.trapz(funcs_dict[l_bound], [l_bound,u_bound])
        
        try:
            timestep_emissions_dict[start_time] += area
        except:
            timestep_emissions_dict[start_time] = area

        return area


def plot_gaussians(mu, sigma, time, showplots=False):
    """ A function to reduce redundant code. Repeated utilities used for plotting the gaussians.
        Here, scipy.stats is used to plot the gaussian and then the gaussian's integral.
    """
    x_range = np.linspace(0, 24, 1000)
    y = ss.norm.cdf(x_range, mu, np.sqrt(sigma))
    z = ss.norm.pdf(x_range, mu, np.sqrt(sigma))

    # Plot P(light a fire) vs temperature
    # where P(light a fire) is the probability of lighting a fire at the inputted time and temperature.
    fig, (ax1, ax2) = plt.subplots(1, 2)

    ax1.plot(x_range, y, c='r')
    ax1.set_ylabel('Probability of lighting a fire')
    ax1.set_xlabel(f'Integral of the gaussian')
    ax1.set_xticks(np.arange(0, 25, 2))
    ax2.set_xlabel(f'Gaussian distribution')
    ax2.plot(x_range, z, c='r')
    ax2.set_xticks(np.arange(0, 25, 2))

    fig.text(0.3, 0.97, f'Normal distribution at {mu}:00 [24H]')

    fig.tight_layout(h_pad=10)

    if showplots:
        plt.show()

    return ss.norm.cdf(time)


def plot_axis(fig, axis, xval, label):
    """ Plots individual axes of the piecewise function, given a figure, function and x array.
    :param fig: Figure that the individual functions get plotted on.
    :param axis: Function to be plotted.
    :param xval: Array of x values, the function's domain.
    :param label: Title of the axes in string form.
    :return: The plotted axis.
    """
    # Function for plotting axes to avoid repetition in plot_fire
    out_axis = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    out_axis.plot(xval, axis, label=label)
    out_axis.autoscale(enable=True)
    out_axis.set_ylim(bottom=0)


def parabola_maker(y_point, x_point, h_vert, k_vert, x):
    """ Takes in equation values.
    :param y_point: This is the height of the parabola, i.e. the y-value of the peak.
    :param x_point: The x-value of the peak of the parabola.
    :param h_vert: The 'width' of the base of the parabola.
    :param k_vert: The 'height' of the parabola.
    :param x: The domain of the parabola.
    :return: Returns equation for parabola passing through points.
    """
    # Used to automate creating equations when plotting the woodburner emissions functions.
    try:
        a = (y_point - k_vert) / (x_point - h_vert) ** 2
        equation = a * (x - h_vert) ** 2 + k_vert
    except ZeroDivisionError:
        equation = 0
    return equation


def gen_city_data(weather_data, outdir, num_meshblocks, temp_var, kelvin=True, census_data=None):
    """ Generates a .csv file that will contain the city data for the model run. The file will have the following
        columns: meshblock number, latitude, longitude, size, number of households
        Where meshblock number is some identification number, I used 001 in the template. Latitude is the latitude
        of the centre of the meshblock, longitude is the same for the longitude. Size refers to the size of the
        meshblock in m^2 and number of households determines how many households the model should simulate per
        meshblock.
        This example .csv is the format of the city data that the model will be expecting.
        The meshblocks in this example is taken from the ECAN census data mentioned in the Confluence writeup.
    """
    weather_template = os.path.join(outdir, 'weather.csv')
    city_template = os.path.join(outdir, 'city_data.csv')
    readme = os.path.join(outdir, 'README.txt')

    ds = xr.open_dataset(weather_data)
    temps = ds[temp_var].to_dataframe()
    temps = temps.resample('H').mean()
    if kelvin:
        temps[temp_var] = (temps[temp_var] - 273.15)
    else:
        pass
    temps.to_csv(weather_template)

    if census_data is None:
        data = {'latitude': np.random.uniform(-43.6058, -43.398, num_meshblocks),
                'longitude': np.random.uniform(172.446, 172.776, num_meshblocks),
                'area': np.random.uniform(1541.19677734, 30404601.6421, num_meshblocks),
                'num_woodburner1': np.random.randint(100, 150, num_meshblocks),
                'num_woodburner2': np.random.randint(100, 150, num_meshblocks),
                'num_woodburner3': np.random.randint(100, 150, num_meshblocks)
                }
    else:
        cen_data = pd.read_excel(census_data)
        longs = cen_data['Centroid Long'].values
        lats = cen_data['Centroid Lat'].values
        areas = cen_data['AREA_M2'].values
        data = {'latitude': longs[0:num_meshblocks],
                'longitude': lats[0:num_meshblocks],
                'area': areas[0:num_meshblocks],
                'num_woodburner1': np.random.randint(0, 20, num_meshblocks),
                'num_woodburner2': np.random.randint(0, 20, num_meshblocks),
                'num_woodburner3': np.random.randint(0, 20, num_meshblocks)
                }

    city_data = pd.DataFrame(data)
    city_data.to_csv(city_template, index=False)

    with open(readme, 'w') as output:
        # I will update the README file as the development of the model progresses.
        outstr = 'Instructions for running the bottom_ip_model.py script.\n\n' \
                 "A quick rundown of the bottom up model:\n" \
                 "The goal of the model is to produce an emissions map of a city.\n" \
                 "This is done by inputting a temperature for the entire city and a time of day.\n" \
                 "This time will be in 24H time to simplify things. The idea is then to create a normal\n" \
                 "distribution of this city-wide temperature. Each household in the city has a certain time\n" \
                 "and a certain temperature at which they usually light their woodburners. This time and\n" \
                 "temperature for eachhousehold is then compared to the city-wide temperature. This is done\n" \
                 "by way of plotting a normal distribution for each household's usual\n" \
                 "fire-lighting temperature. The normal distribution of the city-wide temperature is compared\n" \
                 "with each household's usual fire-lighting temperature's normal distribution. If they overlap\n" \
                 "enough, the household is likely to light their fire at that time. This fire, for simplicity,\n" \
                 "burns until 22:00.Because of this comparison, some households will light their fires at the\n" \
                 "inputted city-wide temperature and time and others won't. These households' woodburners will\n" \
                 "then produce PM2.5 emissions when burning. These are modelled using the Woodburner class in\n" \
                 "the bottom_up_model. These households are then each associated with a meshblock. By\n" \
                 "meshblock, it is meant that the city is divided into a grid of n cells. As such, the physical\n" \
                 "location of each household will be part of some meshblock. These individual household\n" \
                 "emissions modelled by the Woodburner class are then aggregated for the entire meshblock.\n" \
                 "The amount of emissions per meshblock then determines the colour of the meshblock.\n" \
                 "The final product is then an emissions map of differently coloured\n" \
                 "meshblocks for a given time and temperature.\n\n\n" \
                 "HOW TO RUN THE MODEL:\n" \
                 "The model needs the following input files:\n" \
                 "--> city_data.csv\n" \
                 "  This file contains the latitude, longitude, area, num_woodburner1, num_woodburner2, " \
                 "num_woodburner3\n" \
                 "  for each meshblock. As such, each meshblock corresponds to one row, where each column should be\n" \
                 "  filled. Here num_woodburner 1 to 3 refers to how many households in the meshblock has types\n" \
                 "  of woodburners 1 to 3 respectively. See the function '_gen_template_woodburners()' for more\n" \
                 "  information on the types of woodburners currently included in this model.\n" \
                 "--> weather_data.csv\n" \
                 "  This file has two columns: time (datetime 'YYYY-MM-DD HH:MM:SS') and temperature (celsius)\n" \
                 "  Both of these files can be easily generated using the gen_city_data() function.\n" \
                 "From here the following commands will run the model:\n" \
                 "city_data = r'your_directory_here/city_data.csv'\n" \
                 "weather_data = r'/your_directory_here/weather.csv'\n" \
                 "outdir = r'your_directory_here/output'\n" \
                 "control1 = Control(city_data, weather_data, outdir)\n" \
                 "# Uncomment below for plots of the ENTIRE model run:\n" \
                 "# control1.run(10, show_plots=True)\n" \
                 " control1.run(10)\n\n" \
                 "Where control.run() takes as argument the amount of time steps the model should be run for.\n\n" \
                 "Hopefully this model is of use to you!\n" \
                 "Written by Johnny (Shaun) Lowis for Bodeker Scientific."
        output.write(outstr)
    print(f'Finished generating input files in directory: {outdir}')


def gen_census_emissions(shapefile_fp, city_emissions=None):
    """ Function for plotting an emissions map of the ECAN 2013 census data.
    :param shapefile_fp: Filepath to the shapefile of census emissions. This should also include the other files
                         associated with the shapefile.
    :param city_emissions: Filepath to the output location of a successful bottom up model run city_emissions.csv
    :return: An emissions map. Can add in functionality to save to some output directory.
    """
    data = gpd.read_file(shapefile_fp)
    data = data.to_crs("EPSG:3857")

    if city_emissions:
        city_data = pd.read_csv(city_emissions)
        emissions = city_data['emissions']
        data['PM2_5'] = emissions

        fig, ax = plt.subplots(figsize=(12, 8), dpi=400)
        data.plot(ax=ax, column=data['PM2_5'][:], cmap=plt.cm.get_cmap('plasma', 16), legend=True,
                  legend_kwds={'label': r'Emissions of $PM_{2.5}$ [$gm^{-2}$]'}, edgecolor='white',
                  linewidth=0.1)
        # legend_kwds={'label': 'Concentration of PM2.5'}
        plt.title('Bottom up model data PM2.5 emissions per meshblock.', fontsize=18)
        plt.axis('off')
        plt.xlim([19194000, 19235000])
        plt.ylim([-5410000, -5371000])
        ax = plt.gca()
        ctx.add_basemap(ax=ax, crs='EPSG:3857', source=ctx.providers.CartoDB.VoyagerNoLabels)
        plt.tight_layout()
        plt.show()

    else:
        fig, ax = plt.subplots(figsize=(12, 8), dpi=400)
        scaled_emissions = data['PM2_5'][:] / data['geometry'][:].area
        data.plot(ax=ax, column=scaled_emissions, cmap=plt.cm.get_cmap('plasma', 16), legend=True,
                  legend_kwds={'label': r'Emissions of $PM_{2.5}$ [$gm^{-2}$]'}, edgecolor='white',
                  linewidth=0.1)
        # legend_kwds={'label': 'Concentration of PM2.5'}
        plt.title('2013 Census data PM2.5 emissions per meshblock.', fontsize=18)
        plt.axis('off')
        plt.xlim([19194000, 19235000])
        plt.ylim([-5410000, -5371000])
        ax = plt.gca()
        ctx.add_basemap(ax=ax, zoom=11, crs='EPSG:3857', source=ctx.providers.CartoDB.VoyagerNoLabels)
        plt.tight_layout()
        plt.show()


def main():
    """ This is where the model gets called and run from. Examples of how each class works are below, used
        for testing and debugging them while developing the model. See documentation inside the classes for
        further information.
    """
    # --------------------------------------- gen_city_data --------------------------- #
    # Run this code first if you don't have input files ready yet.
    # If your NetCDF file has a different variable name for its temperature data, change the 'temp_var' variable below.
    temp_var = 'air_temperature'
    # outdir = r'/home/shaun/Documents/work_dir/bottom_up/city_data/output'
    outdir = r'/home/shaun/Desktop'
    # weather_data = r'/home/shaun/Documents/work_dir/bottom_up/city_data/' \
    #                r'input/AWS_Metservice_CWX_Christchurch2019_Deployment_raw.nc'
    ethan_netcdf = r'/mnt/temp/Projects/MAPM/Data_Permanent/MAPM_campaign/AWS_QAQCed/Raw/NetCDF/AWS_WOW_Ilam_Christchurch2019_raw.nc'
    
    # Gen data from census data:
    census_data = r'/home/shaun/Documents/work_dir/bottom_up/census_data/MB_plus_census_wood.xlsx'
    gen_city_data(ethan_netcdf, outdir, 50, temp_var, census_data)
    # Gen random data:
    gen_city_data(ethan_netcdf, outdir, 50, temp_var)

    # --------------------------------------- MODEL RUN GUIDE --------------------------------- #
    # Run this code to generate emissions values from your input files.

    # city_data = r'/home/shaun/Documents/work_dir/bottom_up/city_data/input/city_data.csv'
    # weather_data = r'/home/shaun/Documents/work_dir/bottom_up/city_data/input/weather.csv'
    # outdir = r'/home/shaun/Documents/work_dir/bottom_up/city_data/output'

    # control = Control(city_data, weather_data, outdir)
    # # The number in the .run() argument is the number of timesteps provided in the weather.csv file
    # control.run(24)

    # --------------------------------------- gen_census_emissions --------------------------- #
    # Run this code to visualise the emissions values generated above. This also works for previous model
    # runs' output, so you don't have to rerun the model every time you want to visualise its output.

    # fp_census_shp = r'/home/shaun/Documents/work_dir/bottom_up/census_data/shapefile/mb_emission.shp'
    # fp_city_data = r'/home/shaun/Documents/work_dir/bottom_up/city_data/output/city_emissions.csv'
    # # To output the bottom up emissions map:
    # gen_census_emissions(fp_census_shp, fp_city_data)
    # To output the 2013 census emissions map:
    # gen_census_emissions(fp_census_shp)


if __name__ == '__main__':
    main()
