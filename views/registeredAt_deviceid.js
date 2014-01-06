function (doc, meta) {
  if(doc.docType && (doc.docType == "device")) {
    emit(dateToArray(new Date(doc.registeredAt)), doc.status);
  }
}