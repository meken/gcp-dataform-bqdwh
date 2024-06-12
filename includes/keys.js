function tostr(colummn) {
    return `CAST(${colummn} AS STRING)`;
}

function surrogate(columns) {
    let keys = ""
    if (Array.isArray(columns)) {
        let combined = columns.map((c) => tostr(c)).join(",'-',");
        keys = `CONCAT(${combined})`;
    } else {
        keys = tostr(columns);
    }
    return `FARM_FINGERPRINT(${keys})`;
}

module.exports = {
    surrogate
};