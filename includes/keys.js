function cast_to_str(colummn) {
    return `CAST(${colummn} AS STRING)`;
}

function surrogate(...columns) {
    let combined = columns.map((c) => cast_to_str(c)).join(",'-',");
    return `FARM_FINGERPRINT(CONCAT(${combined}))`;
}

module.exports = {
    surrogate
};