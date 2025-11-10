from __future__ import annotations

import typer

from app.services.pipeline import Pipeline

cli = typer.Typer(help="Atualiza o banco de dados com os arquivos mais recentes do CNPJ.")


@cli.command()
def run(release: str | None = typer.Option(None, help="Release no formato YYYY-MM")) -> None:
    pipeline = Pipeline()
    version = pipeline.run(release)
    typer.echo(f"Base atualizada para a versão {version}")


if __name__ == "__main__":
    cli()
