from src.core.service.documents.entity import IDocument
from src.core.service.documents.model import DocumentModel, documentsModel


class DocumentsService:
    def __init__(self, model: DocumentModel):
        self.model = model

    def insert_document(self, document: IDocument) -> None:
        self.model.get_model().insert_one(document)


documentsService = DocumentsService(documentsModel)
