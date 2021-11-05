from django.contrib.postgres.operations import CreateExtension


class HllExtension(CreateExtension):
    # Available for django 1.10+
    # For previous versions use
    #   migrations.RunSQL('CREATE EXTENSION IF NOT EXISTS hll;', reverse_sql='DROP EXTENSION hll;')
    def __init__(self):
        self.name = 'hll'
