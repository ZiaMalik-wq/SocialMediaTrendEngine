import json
from datetime import datetime
import spacy
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext
from filters import is_recent_record

ALLOWED_ENTITY_TYPES = {'PERSON', 'ORG', 'GPE', 'EVENT', 'PRODUCT'}

class EntityExtractor(FlatMapFunction):
    def open(self, runtime_context: RuntimeContext):
        # Load spaCy model once per TaskManager slot (not per record)
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            self.nlp = None

    def flat_map(self, value):
        if not self.nlp:
            return

        try:
            data = json.loads(value)
            if not is_recent_record(data):
                return
            text = data.get('text', '')
            if not text:
                return

            ts  = data.get('ingested_at', datetime.utcnow().isoformat())
            doc = self.nlp(text)

            for ent in doc.ents:
                if ent.label_ in ALLOWED_ENTITY_TYPES:
                    yield (ent.text.strip(), ent.label_, ts)

        except Exception:
            pass  # silently drop malformed records