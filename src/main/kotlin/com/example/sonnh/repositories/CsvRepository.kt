package com.example.sonnh.repositories

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.math.BigInteger
import javax.persistence.EntityManager

@Repository
class CsvRepository {

    @Autowired
    private lateinit var entityManager: EntityManager

    fun createDynamicTable(tableName: String, keys: Set<String>, columns: Set<String>) {
        val query = StringBuilder()
        query.append("DROP TABLE IF EXISTS \"$tableName\";")
        query.append(" Create table \"${tableName}\"")
        query.append(" (")
        query.append(" id serial PRIMARY KEY,")
        for (column in columns) {
            query.append(" $column Text, ")
        }
        query.append(" created_date timestamp,")
        query.append(" updated_date timestamp")
        query.append(" );")

        entityManager.createNativeQuery(query.toString()).executeUpdate()
    }

    fun upsertRow(tableName: String, rowMap: Map<String, Any>, keys: Set<String>) {
        if (isRowExists(tableName, rowMap, keys)) {

        } else {
            val columnTables = rowMap.keys
            val columnTableStr = columnTables.joinToString(",")
            val query = StringBuilder()
            query.append("INSERT INTO \"$tableName\" ")
            query.append("($columnTableStr) ")
            query.append("VALUES ")
            query.append("( ")
            columnTables.forEachIndexed { index, columnTable ->
                query.append(":$columnTable${if (index != columnTables.size - 1) "," else ""} ")
            }
            query.append("); ")
            val nativeQuery = entityManager.createNativeQuery(query.toString())
            for (columnTable in columnTables) {
                nativeQuery.setParameter(columnTable, rowMap[columnTable])
            }
            nativeQuery.executeUpdate()
        }
    }

    private fun isRowExists(tableName: String, rowMap: Map<String, Any>, tableKeys: Set<String>): Boolean {
        val query = StringBuilder()
        query.append("SELECT count(id) FROM \"$tableName\"")
        query.append(" WHERE")
        tableKeys.forEachIndexed { index, key ->
            query.append(" $key = :${key}${if (index != (tableKeys.size - 1)) "," else ""}")
        }
        val nativeQuery = entityManager.createNativeQuery(query.toString())
        for (key in tableKeys) {
            nativeQuery.setParameter(key, rowMap[key])
        }
        return (nativeQuery.singleResult as BigInteger).toInt() > 0
    }
}