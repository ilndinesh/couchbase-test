function (doc, meta) {
  if(doc.docType && (doc.docType == "device")) {
    emit(doc.status, doc.registeredAt);
  }
}