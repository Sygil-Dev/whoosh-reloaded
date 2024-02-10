import gzip
import os.path

from whoosh import analysis, fields, index, qparser, query
from whoosh.support.bench import Bench, Spec
from whoosh.util import now


class Reuters(Spec):
    """
    The Reuters class represents a benchmark for the Reuters dataset.

    Attributes:
        name (str): The name of the benchmark.
        filename (str): The name of the file containing the dataset.
        main_field (str): The main field in the dataset.
        headline_text (str): The field representing the headline text in the dataset.
    """

    name = "reuters"
    filename = "reuters21578.txt.gz"
    main_field = "text"
    headline_text = "headline"

    def whoosh_schema(self):
        """
        Returns the schema for the Whoosh index.

        Returns:
            Schema: The schema for the Whoosh index.
        """
        # ana = analysis.stemming_analyzer()
        ana = analysis.standard_analyzer()
        schema = fields.Schema(
            id=fields.ID(stored=True),
            headline=fields.STORED,
            text=fields.TEXT(analyzer=ana, stored=True),
        )
        return schema

    def zcatalog_setup(self, cat):
        """
        Sets up the ZCatalog index.

        Args:
            cat (ZCatalog): The ZCatalog instance to set up.
        """
        from zcatalog import indexes  # type: ignore @UnresolvedImport

        cat["id"] = indexes.FieldIndex(field_name="id")
        cat["headline"] = indexes.TextIndex(field_name="headline")
        cat["body"] = indexes.TextIndex(field_name="text")

    def documents(self):
        """
        Generates documents from the dataset.

        Yields:
            dict: A document from the dataset.
        """
        path = os.path.join(self.options.dir, self.filename)
        f = gzip.GzipFile(path)

        for line in f:
            id_var, text = line.decode("latin1").split("\t")
            yield {"id": id_var, "text": text, "headline": text[:70]}


if __name__ == "__main__":
    Bench().run(Reuters)
