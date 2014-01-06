function (doc, meta) {
  if(doc.docType && (doc.docType == "device")) {
    for(nodeId in doc.nodeInfoMap) {
      for(cid in doc.nodeInfoMap[nodeId].connInfoMap) {
        emit(dateToArray(new Date(doc.nodeInfoMap[nodeId].connInfoMap[cid].last)), null);
      }
    }
  }
}