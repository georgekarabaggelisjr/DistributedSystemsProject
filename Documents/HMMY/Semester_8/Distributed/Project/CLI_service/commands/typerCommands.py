import typer

# Initialize sub-apps
app = typer.Typer(help="Map-Reduce Kubernetes CLI")
jobs_app = typer.Typer(help="Manage Map-Reduce Jobs")
admin_app = typer.Typer(help="System Administration")

app.add_typer(jobs_app, name="jobs")
app.add_typer(admin_app, name="admin")

@app.command("login")
def login_command(
    username: str = typer.Argument(..., help="Your Keycloak username"),
    password: str = typer.Option(..., prompt=True, hide_input=True, help="Your password")
):
    """
    Authenticates the user with the Keycloak Auth Service and stores the JWT locally.
    """
    pass

@jobs_app.command("submit")
def submit_command(
    data: str = typer.Argument(..., help="Path to the input data file (.txt, .json)"),
    code: str = typer.Argument(..., help="Path to the executable code (.jar, .class)")
):
    """
    Uploads files to MinIO and schedules a new Map-Reduce job.
    """
    pass

@jobs_app.command("status")
def status_command(
    job_id: str = typer.Argument(None, help="Specific Job ID. Leave blank to list all.")
):
    """
    Retrieves the execution status of a specific job or all owned jobs.
    """
    pass

@jobs_app.command("result")
def result_command(
    job_id: str = typer.Argument(..., help="The ID of the completed job"),
    output: str = typer.Option("./output.txt", help="Local path to save the results")
):
    """
    Downloads the final Output File from MinIO (Requires job status to be COMPLETED).
    """
    pass

@admin_app.command("users-create")
def admin_create_user_command(
    username: str = typer.Argument(...),
    password: str = typer.Option(..., prompt=True, hide_input=True)
):
    """
    [ADMIN ONLY] Provisions a new user in the Keycloak Auth Service.
    """
    pass

@admin_app.command("config-workers")
def admin_config_workers_command(
    count: int = typer.Option(..., "--count", help="Maximum number of parallel workers")
):
    """
    [ADMIN ONLY] Updates the DDS configuration and broadcasts changes to Manager instances.
    """
    pass