import asyncio
import random
import threading
import time
from datetime import datetime, timedelta

import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
from tqdm.asyncio import tqdm as tqdm_asyncio

from outages.utils_geo import REGIONS

MAX_HISTORY_LENGTH = 5000  # Maximum length of history for each queue
WINDOW_SIZE = 60*60  # Window size in seconds for the plot

# Custom progress bar class that updates dashboard progress data
class CustomTqdm(tqdm_asyncio):
    def __init__(self, *args, dashboard_progress=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_progress = dashboard_progress
        self.is_closed = False

    def update(self, n=1):
        if not self.is_closed:
            super().update(n)
            # Sync with dashboard_progress
            if self.dashboard_progress is not None:
                with self.dashboard_progress['lock']:
                    self.dashboard_progress['n'] = self.n
                    self.dashboard_progress['total'] = self.total
                    self.dashboard_progress['desc'] = self.desc
            
            # Automatically close when progress reaches total
            if self.n >= self.total:
                self.close()

    def close(self):
        if not self.is_closed:
            # super().disable()
            self.is_closed = True

    def reset(self, total=None, desc=None):
        # Reset the progress bar's state for reuse
        if self.is_closed:
            self.is_closed = False

        # Set a new total, reset progress, description, and start time
        self.n = 0  # Reset progress
        self.total = total if total is not None else self.total
        self.start_t = time.time()  # Reset start time
        if desc is not None:
            self.set_description(desc)
        self.refresh()  # Refresh the display


class DashboardApp:
    def __init__(self, num_queues=10, max_history_length=MAX_HISTORY_LENGTH):
        self.num_queues = num_queues
        self.MAX_HISTORY_LENGTH = max_history_length

        # Shared data structures with thread-safe access
        self.queue_data_lock = threading.Lock()
        self.queue_data = {
            'timestamps': [[] for _ in range(num_queues)],  # List of timestamps for each queue
            'status_codes': [[] for _ in range(num_queues)],  # List of status codes for each queue
            'messages': ['' for _ in range(num_queues)]  # Most recent message for each queue
        }

        # Initialize progress data with thread-safe access
        self.progress_data = [
            {'n': 0, 'total': 0, 'desc': '', 'lock': threading.Lock()}
            for _ in range(2 * len(REGIONS))
        ]

        # Initialize custom tqdm progress bars
        self._tqdm_pbars = []
        for i, region in enumerate(REGIONS):
            # Progress bar for region progress
            dashboard_progress = self.progress_data[i * 2]
            region_pbar = CustomTqdm(
                total=0,
                position=i * 2,
                dashboard_progress=dashboard_progress
            )
            region_pbar.set_description(f'{region.name} - region progress')
            self._tqdm_pbars.append(region_pbar)

            # Progress bar for event details progress
            dashboard_progress = self.progress_data[i * 2 + 1]
            events_pbar = CustomTqdm(
                total=0,
                position=i * 2 + 1,
                dashboard_progress=dashboard_progress
            )
            events_pbar.set_description(f'{region.name} - event details progress')
            self._tqdm_pbars.append(events_pbar)

        # Initialize the Dash app with Bootstrap CSS
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

        # Layout definition
        self.app.layout = dbc.Container([
            # Refresh Button Row
            dbc.Row([
                dbc.Col([
                    dbc.Button("Refresh Data", id="refresh-button", color="primary", style={"margin": "10px"})
                ], width=12)
            ]),

            # Progress Bars Row
            dbc.Row([
                dbc.Col([
                    html.H4('Progress Bars'),
                    dbc.Row([
                        dbc.Col([
                            dbc.Progress(
                                id=f'progress-bar-{region_index * 2 + i}',
                                value=0,
                                label=f'{region.name} - region progress' if i == 0 else f'{region.name} - event details progress',
                                style={'margin': '10px'}
                            ) for region_index, region in enumerate(REGIONS) for i in range(2)
                        ])
                    ])
                ], width=12)
            ]),

            # Status Messages Over Time Plot Row
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id='history-plot', style={'height': '500px'})
                ], width=12)
            ]),

            # Tabs for Queue Messages Row
            dbc.Row([
                dbc.Col([
                    dcc.Tabs(id='queue-tabs', value='tab-0', children=[
                        dcc.Tab(label=f'Queue {i}', value=f'tab-{i}') for i in range(self.num_queues)
                    ]),
                    html.Div(id='queue-messages')
                ], width=12)
            ]),

            dcc.Store(id='refresh-store')
        ], fluid=True)

        # Setup the callbacks
        outputs = [
            Output('history-plot', 'figure'),
            Output('queue-messages', 'children'),
        ] + [
            Output(f'progress-bar-{region_index * 2 + i}', 'value')
            for region_index, region in enumerate(REGIONS) for i in range(2)
        ] + [
            Output(f'progress-bar-{region_index * 2 + i}', 'label')
            for region_index, region in enumerate(REGIONS) for i in range(2)
        ]

        self.app.callback(
            outputs,
            [Input('queue-tabs', 'value'),  # Trigger on tab change
             Input('refresh-button', 'n_clicks')],
            [State('queue-tabs', 'value')]
        )(self.update_dashboard)

        self.app.callback(
            Output('refresh-store', 'data'),
            [Input('refresh-button', 'n_clicks')]
        )(self.trigger_refresh)

    def update_dashboard(self, selected_tab, n_clicks, state):
        queue_index = int(selected_tab.split('-')[-1])  # Extract queue index from selected tab

        with self.queue_data_lock:
            # Plot Data
            plot_data = []
            colors = ['red', 'green', 'blue', 'orange']  # Colors for status codes 0 to 3
            for i in range(self.num_queues):
                x = [datetime.fromtimestamp(ts) for ts in self.queue_data['timestamps'][i]]
                y = [i] * len(x)  # Horizontal line at y=i
                status_codes = self.queue_data['status_codes'][i]
                marker_colors = [colors[code % len(colors)] for code in status_codes]
                trace = go.Scatter(
                    x=x,
                    y=y,
                    mode='markers',
                    marker=dict(
                        color=marker_colors,
                        size=10
                    ),
                    name=f'Queue {i}'
                )
                plot_data.append(trace)

            # Create the layout for the plot
            layout = go.Layout(
                yaxis=dict(
                    tickvals=list(range(self.num_queues)),
                    ticktext=[f'Queue {i}' for i in range(self.num_queues)],
                    range=[-1, self.num_queues]
                ),
                xaxis=dict(
                    type='date',
                    range=[datetime.now() - timedelta(seconds=WINDOW_SIZE), datetime.now()],
                    title='Time'
                ),
                showlegend=True,
                height=500
            )

            fig = go.Figure(data=plot_data, layout=layout)

            # Create the message list for the selected queue tab
            messages = [html.Div(f'Queue {queue_index}: {self.queue_data["messages"][queue_index]}', style={'padding': '5px'})]

        # Get progress values and labels
        progress_percentages = []
        progress_labels = []
        for i, progress in enumerate(self.progress_data):
            with progress['lock']:
                pbar = self._tqdm_pbars[i]
                n = pbar.n
                total = pbar.total
                format_dict = pbar.format_dict
                format_dict['bar_format'] = '{n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
                desc = progress['desc'] + pbar.format_meter(**format_dict)
            percentage = (n / total * 100) if total > 0 else 0
            progress_percentages.append(percentage)
            progress_labels.append(desc)

        # Return all outputs (plot, messages, progress bar values, and labels)
        return [fig, messages] + progress_percentages + progress_labels

    def trigger_refresh(self, n_clicks):
        # Use this callback to trigger the update of data by storing the current time in 'refresh-store'
        return time.time()

    def run(self, **kwargs):
        # Start the Dash app
        self.app.run_server(**kwargs)

    # Method for queues to send updates
    def send_update(self, queue_index, status_code, message):
        timestamp = time.time()
        with self.queue_data_lock:
            self.queue_data['timestamps'][queue_index].append(timestamp)
            self.queue_data['status_codes'][queue_index].append(status_code)
            if message is not None:
                message += f' at {time.strftime("%H:%M:%S")}'
            self.queue_data['messages'][queue_index] = message
            # Limit the history length
            if len(self.queue_data['timestamps'][queue_index]) > self.MAX_HISTORY_LENGTH:
                self.queue_data['timestamps'][queue_index] = self.queue_data['timestamps'][queue_index][-self.MAX_HISTORY_LENGTH:]
                self.queue_data['status_codes'][queue_index] = self.queue_data['status_codes'][queue_index][-self.MAX_HISTORY_LENGTH:]

    # Method to set total length N for progress bars
    def pbar_total(self, index, N, desc=None):
        pbar = self._tqdm_pbars[index]
        with self.progress_data[index]['lock']:
            pbar.reset(total=N, desc=desc)  # Leverage the new reset method

    # Method to update progress bars (increments progress by n)
    def pbar_update(self, index, n=1):
        pbar = self._tqdm_pbars[index]
        pbar.update(n)
        pbar.refresh()  # Ensure the progress bar state is updated


# Simulated tasks to demonstrate progress bar functionality
async def simulated_task(dashboard, index):
    while True:
        # Set total length N for the progress bar with description
        total_steps = random.randint(50, 150)
        desc = f"Task {index}"
        dashboard.pbar_total(index, total_steps, desc=desc)
        # Simulate work
        for _ in range(total_steps):
            # Simulate work being done
            await asyncio.sleep(random.uniform(0.05, 0.2))
            # Update progress bar
            dashboard.pbar_update(index)
        # Reset progress for demonstration after completion
        await asyncio.sleep(2)
        dashboard.pbar_total(index, 0)


# Simulated async task queues
async def queue_task(queue_index, dashboard):
    while True:
        # Simulate task duration
        await asyncio.sleep(random.uniform(0.5, 5))
        # Generate status code and message
        status_code = random.randint(0, 3)  # Status codes between 0 and 3
        message = f"Queue {queue_index} message at {time.strftime('%H:%M:%S')}"
        # Send update to the dashboard
        dashboard.send_update(queue_index, status_code, message)


def start_background_tasks(dashboard):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = [loop.create_task(queue_task(i, dashboard)) for i in range(dashboard.num_queues)]
    tasks += [loop.create_task(simulated_task(dashboard, i)) for i in range(2 * len(REGIONS))]
    loop.run_forever()


if __name__ == '__main__':
    dashboard = DashboardApp(num_queues=10)
    threading.Thread(target=start_background_tasks, args=(dashboard,), daemon=True).start()
    dashboard.run(debug=True, use_reloader=False)
