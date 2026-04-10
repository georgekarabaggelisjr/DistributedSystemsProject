import typer
from dotenv import load_dotenv
from core.LocalStateManager import LocalStateManager
from commands.typerCommands import jobs_app, admin_app
from clients.AuthClient import AuthClient

load_dotenv()

app = typer.Typer(
    help="MapReduce Distributed System CLI - Group 10",
    rich_markup_mode="rich"
)

# 3. Attach sub-modules (the jobs and admin command groups)
app.add_typer(jobs_app, name="jobs")
app.add_typer(admin_app, name="admin")


@app.command()
def login(
        username: str = typer.Argument(..., help="Keycloak Username"),
        password: str = typer.Option(..., prompt=True, hide_input=True)
):
    """
    Authenticate with Keycloak to receive a JWT token.
    """
    state = LocalStateManager()
    auth = AuthClient(state)

    if auth.login(username, password):
        typer.secho(f"Successfully logged in as {username}!", fg=typer.colors.GREEN)
    else:
        typer.secho("Login failed. Check credentials.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()