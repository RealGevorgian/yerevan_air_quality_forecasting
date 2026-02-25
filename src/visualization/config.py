import matplotlib.pyplot as plt

# Visualization settings
PLOT_STYLE = 'seaborn-v0_8-darkgrid'
FIGURE_SIZE = (15, 10)
DPI = 150

# Color schemes
COLORS = {
    'good': 'green',
    'moderate': 'yellow',
    'unhealthy_sensitive': 'orange',
    'unhealthy': 'red',
    'very_unhealthy': 'purple',
    'hazardous': 'maroon'
}

# WHO guideline lines
WHO_GUIDELINES = {
    'annual': 5,
    '24h': 15,
    'interim_1': 35,
    'interim_2': 25,
    'interim_3': 15,
    'interim_4': 10
}

def setup_plotting_style():
    """Set up default plotting style."""
    plt.style.use(PLOT_STYLE)