import typer
from clients.JobClient import JobClient
from clients.AdminClient import AdminClient
from core.LocalStateManager import LocalStateManager
state_manager = LocalStateManager()

jobs_app = typer.Typer(help="Manage Map-Reduce Jobs")
admin_app = typer.Typer(help="System Administration")

@jobs_app.command("submit")
def submit_command(
    data: str = typer.Argument(..., help="Path to the input data file (.txt, .json)"),
    code: str = typer.Argument(..., help="Path to the executable code (.jar, .class)")
):
    """
    Uploads files to MinIO and schedules a new Map-Reduce job.
    """
    client = JobClient(state_manager)
    typer.echo(f"📤 Submitting files: {data} and {code}...")

    try:
        job_id = client.submit_job(data, code)
        typer.secho(f"✅ Success! Job ID: {job_id}", fg=typer.colors.GREEN, bold=True)
    except Exception as e:
        typer.secho(f"❌ Error: {e}", fg=typer.colors.RED)

@jobs_app.command("status")
def status_command(
    job_id: str = typer.Argument(None, help="Specific Job ID. Leave blank to list all.")
):
    """
    Retrieves the execution status of a specific job or all owned jobs.
    """
    client = JobClient(state_manager)
    result = client.get_job_status(job_id)

    if isinstance(result, list):
        typer.secho("\n📋 Jobs list:", bold=True)
        for job in result:
            typer.echo(f"ID: {job['id']} | Status: {job['status']}")
    else:
        typer.secho(f"\n🔍 Job Status {job_id}:", bold=True)
        typer.echo(f"Status: {result.get('status')}")
        typer.echo(f"Created: {result.get('created_at')}")

@jobs_app.command("result")
def result_command(
    job_id: str = typer.Argument(..., help="The ID of the completed job"),
    output: str = typer.Option("./output.txt", help="Local path to save the results")
):
    """
    Downloads the final Output File from MinIO (Requires job status to be COMPLETED).
    """
    client = JobClient(state_manager)
    typer.echo(f"⬇️ Λήψη αποτελεσμάτων για το job {job_id}...")
    client.download_result(job_id, output)

# --- ADMIN COMMANDS ---

@admin_app.command("users-create")
def admin_create_user_command(
        username: str = typer.Argument(..., help="The username for the new user"),
        email: str = typer.Argument(..., help="The email for the new user"),
        password: str = typer.Option(..., prompt=True, hide_input=True)
):
    client = AdminClient(state_manager)
    details = {"username": username, "email": email, "password": password}

    user_id = client.create_user(details)
    typer.secho(f"✅ User created! Keycloak ID: {user_id}", fg=typer.colors.GREEN)


@admin_app.command("config-workers")
def admin_config_workers_command(count: int):
    client = AdminClient(state_manager)
    if client.configure_workers(count):
        typer.secho(f"⚙️ System updated to {count} workers.", fg=typer.colors.GREEN)
        typer.secho("✅ Setting configured (Mock)", fg=typer.colors.GREEN)