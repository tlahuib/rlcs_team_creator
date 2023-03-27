import re

import numpy as np
import plotly.graph_objects as go

# Plotting format

image_path = 'C:/Users/tabm9/OneDrive - Caissa Analytica/Documents/DS_Portfolio/Projects/Images/RLCS/'

line_color = '#f1f5f9'
font_color = '#f1f5f9'
background_color = '#032137'

layout_dict = dict(
    font_color=font_color,
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis=dict(gridcolor=line_color, linecolor=line_color,
               zerolinecolor=line_color),
    yaxis=dict(gridcolor=line_color, linecolor=line_color,
               zerolinecolor=line_color),
    legend=dict(x=0, y=1, bgcolor=background_color),
    margin=dict(r=0, l=0, b=0)
)

tablet_args = dict(default_width='700px', default_height='300px')
mobile_args = dict(default_width='400px', default_height='175px')


def write_plot(fig: go.Figure, file_name: str, path: str = image_path, **kwargs):
    # Transform into html
    html = fig.to_html(include_plotlyjs='cdn', div_id=file_name, **kwargs)

    # Extract js code
    js_start = [s.end() for s in re.finditer(
        '<script type="text/javascript">', html)][-1]
    js_end = [s.start() for s in re.finditer('</script>', html)][-1]

    js = html[js_start: js_end]

    # Write files
    with open(path + file_name + '.js', 'w') as f:
        f.write(js)


# Random Walks

def bound_array(a, bounds=[0, 1]):
    s = a.copy()

    # Check number of times it breaks bounds
    signed_crosses = np.floor(
        (s - bounds[0]) / (bounds[1] - bounds[0])).astype(int)
    crosses = np.floor(np.where(signed_crosses < 0, -
                       signed_crosses, signed_crosses)).astype(int)

    # Reorient direction
    s *= ((-1) ** crosses)

    # Reposition to maintain continuity
    s += (bounds[1] - bounds[0]) * signed_crosses * (-1) ** (crosses + 1)
    s += (bounds[1] + bounds[0]) * 0 ** ((crosses + 1) % 2)

    return s


def non_stationary_bounded_random_walk(initial=1/2, bounds=[0, 1], size=[1000, 1], drift=0, std=0.001, std_bounds=[0, 0.05]):
    # Generate stds
    std_array = abs(np.random.normal(
        loc=drift, scale=std, size=size).cumsum(0) + std)
    std_array = bound_array(std_array, std_bounds)

    # Generate random walk initialized on 0
    walk = np.random.normal(loc=drift, scale=std_array,
                            size=size).cumsum(0) + initial
    walk = bound_array(walk, bounds)

    return walk


# X-data formatting

def apply_rolling_function(array, window, func):
    shape = list(array.shape)
    res = np.zeros(shape)

    for i in range(shape[0]):
        res[i] = func(
            array[max(0, i - window):min(shape[0], i + window)], axis=0)

    return res


def step_mean(array, alpha, init=0.5):
    N = array.shape
    means = alpha * np.ones(N) * init + (1 - alpha) * array[0]

    for i in range(1, N[0]):
        means[i] = alpha * means[i - 1] + (1 - alpha) * array[i]

    return means


def generate_X(games_results: np.array, windows: list = [5, 10, 20, 50, 100], alphas: list = [0.85, 0.9, 0.95, 0.975, 0.99]):
    # Generate indexes
    shape = games_results.shape
    random_walk_ids = np.log(
        1 + np.array(list(range(shape[0])) * shape[1])).tolist()

    # Calculate window means, stds
    window_means = np.empty([len(windows), 0]).tolist()
    window_stds = np.empty([len(windows), 0]).tolist()
    for i in range(len(windows)):
        window_means[i] = apply_rolling_function(
            games_results, windows[i], np.mean).T
        window_stds[i] = apply_rolling_function(
            games_results, windows[i], np.std).T

    # Calculate step means, stds
    step_means = np.empty([len(alphas), 0]).tolist()
    step_stds = np.empty([len(alphas), 0]).tolist()
    for i in range(len(alphas)):
        step_means[i] = step_mean(games_results, alphas[i])
        step_stds[i] = apply_rolling_function(step_means[i], 10, np.std).T
        step_means[i] = step_means[i].T
    X = np.array(
        random_walk_ids +
        np.array(games_results).flatten().tolist() +
        np.array(window_means).flatten().tolist() +
        np.array(window_stds).flatten().tolist() +
        np.array(step_means).flatten().tolist() +
        np.array(step_stds).flatten().tolist()
    ).reshape([2 + 2 * len(windows) + 2 * len(alphas), -1])

    return X
