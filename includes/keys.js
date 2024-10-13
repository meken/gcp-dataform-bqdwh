function cast_to_str(colummn) {
    return `CAST(${colummn} AS STRING)`;
}

/**
 * Generates a surrogate key by hashing the (stringified) values of the provided set of columns
 * 
 * @param  {...string} columns 
 * @returns 
 */
function surrogate(...columns) {
    let combined = columns.map((c) => cast_to_str(c)).join(",'-',");
    return `FARM_FINGERPRINT(CONCAT(${combined}))`;
}

/**
 * Generates idempotently a primary key reference for the source table (`ctx.self()`) using the provided column
 * 
 * @param {*} ctx context for accessing Dataform built-in functions
 * @param {string} column the primary key column
 */
function primary(ctx, column) {
    return `ALTER TABLE ${ctx.self()} DROP PRIMARY KEY IF EXISTS, \
        ADD PRIMARY KEY (${column}) NOT ENFORCED;`;
}

/**
 * Generates idempotently a foreign key reference from the source table (`ctx.self()`) to the target table
 * 
 * @param {*} ctx context for accessing Dataform built-in functions
 * @param {string} target_table name of the table which contains the referenced column
 * @param {string} target_column name of the referenced column
 * @param {string} src_column (optional) name of the reference column, if not defined it's assumed 
 * to be the same as the referenced column
 */
function foreign(ctx, target_table, target_column, src_column) {
    if (!src_column) {
        src_column = target_column;
    }
    return `ALTER TABLE ${ctx.self()} ADD CONSTRAINT IF NOT EXISTS fk_${src_column} \
        FOREIGN KEY (${src_column}) REFERENCES ${target_table}(${target_column}) NOT ENFORCED;`;
}

module.exports = {
    surrogate,
    primary,
    foreign
};