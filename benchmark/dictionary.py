import gzip
import os

from whoosh import analysis, fields
from whoosh.support.bench import Bench, Spec


class VulgarTongue(Spec):
    """
    A class representing a VulgarTongue dictionary.

    Attributes:
        name (str): The name of the dictionary.
        filename (str): The filename of the dictionary file.
        headline_field (str): The field name for the headline.
    """

    name = "dictionary"
    filename = "dcvgr10.txt.gz"
    headline_field = "head"

    def documents(self):
        """
        Generator function that yields documents from the dictionary file.

        Yields:
            dict: A dictionary representing a document with 'head' and 'body' fields.
        """
        path = os.path.join(self.options.dir, self.filename)
        f = gzip.GzipFile(path)

        head = body = None
        for line in f:
            line = line.decode("latin1")
            if line[0].isalpha():
                if head:
                    yield {"head": head, "body": head + body}
                head, body = line.split(".", 1)
            else:
                body += line

        if head:
            yield {"head": head, "body": head + body}

    def whoosh_schema(self):
        """
        Returns the Whoosh schema for the VulgarTongue dictionary.

        Returns:
            Schema: The Whoosh schema for the dictionary.
        """
        ana = analysis.stemming_analyzer()

        schema = fields.Schema(
            head=fields.ID(stored=True), body=fields.TEXT(analyzer=ana, stored=True)
        )
        return schema

    def zcatalog_setup(self, cat):
        """
        Sets up the ZCatalog indexes for the VulgarTongue dictionary.

        Args:
            cat (ZCatalog): The ZCatalog instance.
        """
        from zcatalog import indexes  # type: ignore @UnresolvedImport

        cat["head"] = indexes.FieldIndex(field_name="head")
        cat["body"] = indexes.TextIndex(field_name="body")


if __name__ == "__main__":
    Bench().run(VulgarTongue)
