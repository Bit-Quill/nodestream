from .operation import Operation, NodestreamCommand
from ...project import Target
from ...schema.migrations import ProjectMigrations


class ExecuteMigrations(Operation):
    def __init__(self, migrations: ProjectMigrations, target: Target):
        self.migrations = migrations
        self.target = target

    async def perform(self, command: NodestreamCommand):
        migrator = self.target.make_migrator()
        with command.spin(
            f"Executing migrations on target {self.target.name}...",
            f"Migrations executed on target {self.target.name}.",
        ):
            async for migration in self.migrations.execute_pending(migrator):
                command.info(f"Migration {migration.name} executed successfully.")
