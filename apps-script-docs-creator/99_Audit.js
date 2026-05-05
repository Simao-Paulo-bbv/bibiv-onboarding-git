function auditAgreementTemplatePlaceholders() {
  const templateIds = [
    "1GnZM7kU2l2CzWBnSI2Z1VCL03VZarh6cO0_vxQVVLTU",
    "1KEUT-51DRypLNgXwSX7-eRkdZQvyRaEx2CGj05hi21s",
    "1AR2NkizWPcazbQFAAj9W89m4ZBnQS_wv2VIoYoLIkBg",
    "1aruVKM0vlyvcMfOFkZsKLL-RqsBxyMJqvwYKchkgH0Q",
    "1r8hZukGAlg7V0O8mk2v3SdTfgu1CL1PpMrv2MpQ8dBc"
  ];

  const result = templateIds.map(id => {
    const doc = DocumentApp.openById(id);
    const text = getPrimaryDocumentSections_(doc).map(section => section.getText()).join("\n");
    const placeholders = uniqueMatches_(text, /<<[\s\S]*?>>/g);
    return {
      id: id,
      name: DriveApp.getFileById(id).getName(),
      placeholders: placeholders
    };
  });

  console.log(JSON.stringify(result, null, 2));
  return result;
}

function uniqueMatches_(text, pattern) {
  const seen = {};
  const out = [];
  const matches = String(text || "").match(pattern) || [];
  matches.forEach(match => {
    const clean = match.replace(/\s+/g, " ").trim();
    if (seen[clean]) return;
    seen[clean] = true;
    out.push(clean);
  });
  return out.sort();
}
