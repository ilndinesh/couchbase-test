function (doc, meta) {
  if(doc.docType && (doc.docType == "device")) {
    if(doc.busy) {
      emit(doc.busy, null);
    }
  }
}