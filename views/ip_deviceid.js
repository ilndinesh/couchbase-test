function (doc, meta) {
  if(doc.docType && (doc.docType == "device")) {
    for(nodeId in doc.nodeInfoMap) {
      for(cid in doc.nodeInfoMap[nodeId].connInfoMap) {
        emit(doc.nodeInfoMap[nodeId].connInfoMap[cid].ip, null);
      }
    }
  }
}