import _ from 'lodash';
import {
  hasWhiteSpace,
  shouldPrintSchema,
  buildJunctionFields1,
  buildJunctionFields2,
  buildNewTableName,
} from './utils';
import { DEFAULT_SCHEMA_NAME } from '../model_structure/config';

class SpannerExporter {
  static exportEnums(enumIds, model) {
    const enumArr = enumIds.map((enumId) => {
      const _enum = model.enums[enumId];
      const schema = model.schemas[_enum.schemaId];

      const enumValueArr = _enum.valueIds.map((valueId) => {
        const value = model.enumValues[valueId];
        return `  '${value.name}'`;
      });
      const enumValueStr = enumValueArr.join(',\n');

      const line = `CREATE TYPE ${shouldPrintSchema(schema, model)
        ? `"${schema.name}".` : ''}"${_enum.name}" AS ENUM (\n${enumValueStr}\n);\n`;
      return line;
    });

    return enumArr;
  }

  static getFieldLines(tableId, model) {
    const table = model.tables[tableId];

    const lines = table.fieldIds.map((fieldId) => {
      const field = model.fields[fieldId];

      let line = '';

      let schemaName = '';
      if (field.type.schemaName && field.type.schemaName !== DEFAULT_SCHEMA_NAME) {
        schemaName = hasWhiteSpace(field.type.schemaName) ? `"${field.type.schemaName}".` : `${field.type.schemaName}.`;
      }
      const typeName = hasWhiteSpace(field.type.type_name) ? `"${field.type.type_name}"` : field.type.type_name;

      // //Validate typeName is within list of supported types
      // const supportedTypes = ['ARRAY', 'STRING', 'BYTES', 'INT64', 'JSON', 'BOOL', 'DATE', 'TIMESTAMP', 'ARRAY', 'STRUCT', 'ENUM', 'PROTO', 'FLOAT32', 'FLOAT64', 'NUMERIC'];
      // if (!supportedTypes.includes(typeName)) {
      //   //TODO extract size from STRING AND BYTES and validate
      //   if (!(typeName.includes('STRING') || typeName.includes('BYTES'))) {
      //     throw new Error(`Type ${typeName} is not supported in Spanner`);
      //   }          
      // }

      line = `${field.name} ${schemaName}${typeName}`;

      if (field.unique) {
        line += ' UNIQUE';
      }
      if (field.pk) {
        table.nonCompositePrimaryKey = `PRIMARY KEY (${field.name})`;
      }
      if (field.not_null) {
        line += ' NOT NULL';
      }
      if (field.dbdefault) {
        if (field.dbdefault.type === 'expression') {
          line += ` DEFAULT (${field.dbdefault.value})`;
        } else if (field.dbdefault.type === 'string') {
          line += ` DEFAULT '${field.dbdefault.value}'`;
        } else {
          line += ` DEFAULT ${field.dbdefault.value}`;
        }
      }
      return line;
    });

    return lines;
  }

  //TODO, validate columns for keys are not FLOAT32, ARRAY, JSON, STRUCT

  static getCompositePKs(tableId, model) {
    const table = model.tables[tableId];

    const compositePkIds = table.indexIds ? table.indexIds.filter(indexId => model.indexes[indexId].pk) : [];
    const lines = compositePkIds.map((keyId) => {
      const key = model.indexes[keyId];
      let line = 'PRIMARY KEY';
      const columnArr = [];

      key.columnIds.forEach((columnId) => {
        const column = model.indexColumns[columnId];
        let columnStr = '';
        if (column.type === 'expression') {
          columnStr = `(${column.value})`;
        } else {
          columnStr = `${column.value}`;
        }
        columnArr.push(columnStr);
      });

      line += ` (${columnArr.join(', ')})`;

      return line;
    });

    return lines;
  }

  static getTableContentArr(tableIds, model) {
    const tableContentArr = tableIds.map((tableId) => {
      const fieldContents = SpannerExporter.getFieldLines(tableId, model);
      const compositePKs = SpannerExporter.getCompositePKs(tableId, model);

      return {
        tableId,
        fieldContents,
        compositePKs,
      };
    });

    return tableContentArr;
  }

  static getTableContent(tableId, model) {
    const fieldContents = SpannerExporter.getFieldLines(tableId, model);
    const compositePKs = SpannerExporter.getCompositePKs(tableId, model);
    return {
      tableId,
      fieldContents,
      compositePKs,
    };
  }

  static exportTable(tableId, model) {
    const tableContent = SpannerExporter.getTableContent(tableId, model);
    const content = [...tableContent.fieldContents];
    const table = model.tables[tableContent.tableId];
    const primaryKey = tableContent.compositePKs.length === 0 ? table.nonCompositePrimaryKey : tableContent.compositePKs;
    const schema = model.schemas[table.schemaId];
    const tableStr = `CREATE TABLE ${shouldPrintSchema(schema, model)
      ? `${schema.name}.` : ''}${table.name} (\n${content.map(line => `  ${line}`).join(',\n')},\n) ${primaryKey}`;
    const tableEnd = table.interleave?.length > 0 ? `,\n${table.interleave};\n` : ';\n';
    return `${tableStr}${tableEnd}`;
  }

  static buildFieldName(fieldIds, model) {
    const fieldNames = fieldIds.map(fieldId => `${model.fields[fieldId].name}`).join(', ');
    return `(${fieldNames})`;
  }

  static buildTableManyToMany(firstTableFieldsMap, secondTableFieldsMap, tableName, refEndpointSchema, model) {
    let line = `CREATE TABLE ${shouldPrintSchema(refEndpointSchema, model)
      ? `"${refEndpointSchema.name}".` : ''}"${tableName}" (\n`;
    const key1s = [...firstTableFieldsMap.keys()].join('", "');
    const key2s = [...secondTableFieldsMap.keys()].join('", "');
    firstTableFieldsMap.forEach((fieldType, fieldName) => {
      line += `  "${fieldName}" ${fieldType},\n`;
    });
    secondTableFieldsMap.forEach((fieldType, fieldName) => {
      line += `  "${fieldName}" ${fieldType},\n`;
    });
    line += ')';
    line += `  PRIMARY KEY (${key1s}, ${key2s})\n\n;`;
    return line;
  }

  static buildForeignKeyManyToMany(fieldsMap, foreignEndpointFields, refEndpointTableName, foreignEndpointTableName, refEndpointSchema, foreignEndpointSchema, model) {
    const refEndpointFields = [...fieldsMap.keys()].join('", "');
    const line = `ALTER TABLE ${shouldPrintSchema(refEndpointSchema, model)
      ? `"${refEndpointSchema.name}".` : ''}"${refEndpointTableName}" ADD FOREIGN KEY ("${refEndpointFields}") REFERENCES ${shouldPrintSchema(foreignEndpointSchema, model)
        ? `"${foreignEndpointSchema.name}".` : ''}"${foreignEndpointTableName}" ${foreignEndpointFields};\n\n`;
    return line;
  }

  static exportRefs(refIds, model, usedTableNames) {
    const strArr = refIds.map((refId) => {
      let line = '';
      const ref = model.refs[refId];
      const refOneIndex = ref.endpointIds.findIndex(endpointId => model.endpoints[endpointId].relation === '1');
      const refEndpointIndex = refOneIndex === -1 ? 0 : refOneIndex;
      const foreignEndpointId = ref.endpointIds[1 - refEndpointIndex];
      const refEndpointId = ref.endpointIds[refEndpointIndex];
      const foreignEndpoint = model.endpoints[foreignEndpointId];
      const refEndpoint = model.endpoints[refEndpointId];

      const refEndpointField = model.fields[refEndpoint.fieldIds[0]];
      const refEndpointTable = model.tables[refEndpointField.tableId];
      const refEndpointSchema = model.schemas[refEndpointTable.schemaId];
      const refEndpointFieldName = this.buildFieldName(refEndpoint.fieldIds, model, 'spanner');

      const foreignEndpointField = model.fields[foreignEndpoint.fieldIds[0]];
      const foreignEndpointTable = model.tables[foreignEndpointField.tableId];
      const foreignEndpointSchema = model.schemas[foreignEndpointTable.schemaId];
      const foreignEndpointFieldName = this.buildFieldName(foreignEndpoint.fieldIds, model, 'spanner');

      if (refOneIndex === -1) { // many to many relationship
        const firstTableFieldsMap = buildJunctionFields1(refEndpoint.fieldIds, model);
        const secondTableFieldsMap = buildJunctionFields2(foreignEndpoint.fieldIds, model, firstTableFieldsMap);

        const newTableName = buildNewTableName(refEndpointTable.name, foreignEndpointTable.name, usedTableNames);
        line += this.buildTableManyToMany(firstTableFieldsMap, secondTableFieldsMap, newTableName, refEndpointSchema, model);

        line += this.buildForeignKeyManyToMany(firstTableFieldsMap, refEndpointFieldName, newTableName, refEndpointTable.name, refEndpointSchema, refEndpointSchema, model);
        line += this.buildForeignKeyManyToMany(secondTableFieldsMap, foreignEndpointFieldName, newTableName, foreignEndpointTable.name, refEndpointSchema, foreignEndpointSchema, model);
      } else {
        line = `ALTER TABLE ${shouldPrintSchema(foreignEndpointSchema, model)
          ? `${foreignEndpointSchema.name}".` : ''}${foreignEndpointTable.name}\n  ADD `;
        if (ref.name) { line += `CONSTRAINT ${ref.name} `; }
        line += `FOREIGN KEY ${foreignEndpointFieldName} REFERENCES ${shouldPrintSchema(refEndpointSchema, model)
          ? `${refEndpointSchema.name}.` : ''}${refEndpointTable.name} ${refEndpointFieldName}`;
        if (ref.onDelete) {
          line += ` ON DELETE ${ref.onDelete.toUpperCase()}`;
        }
        line += ';\n';
      }
      return line;
    });

    return strArr;
  }

  static exportIndex(indexId, model) {
    // exclude composite pk index
    if (model.indexes[indexId].pk) {
      return '';
    }

    const index = model.indexes[indexId];
    const table = model.tables[index.tableId];
    const schema = model.schemas[table.schemaId];

    let line = 'CREATE';
    if (index.unique) {
      line += ' UNIQUE';
    }
    const indexName = index.name ? `${index.name}` : '';
    line += ' INDEX';
    if (indexName) {
      line += ` ${indexName}`;
    }
    line += ` ON ${shouldPrintSchema(schema, model)
      ? `${schema.name}.` : ''}${table.name}`;
    if (index.type) {
      line += ` USING ${index.type.toUpperCase()}`;
    }

    const columnArr = [];
    index.columnIds.forEach((columnId) => {
      const column = model.indexColumns[columnId];
      let columnStr = '';
      if (column.type === 'expression') {
        columnStr = `(${column.value})`;
      } else {
        columnStr = `"${column.value}"`;
      }
      columnArr.push(columnStr);
    });

    line += ` (${columnArr.join(', ')})`;
    line += ';';

    return line;
  }

  static exportTableAndIndexes(tableId, model) {
    const tableStr = SpannerExporter.exportTable(tableId, model);
    const indexStrs = model.tables[tableId].indexIds.map((indexId) => SpannerExporter.exportIndex(indexId, model));
    return [tableStr, ...indexStrs];
  }

  static exportComments(comments, model) {
    const commentArr = comments.map((comment) => {
      let line = 'COMMENT ON';
      const table = model.tables[comment.tableId];
      const schema = model.schemas[table.schemaId];
      switch (comment.type) {
        case 'table': {
          // If comment starts with INTERLEAVE IN PARENT set on table and return empty line, we are hijack comments for some spanner stuff
          if (table.note.includes('INTERLEAVE IN PARENT')) {
            table.interleave = table.note;
            return '';
          }
          line += ` TABLE ${shouldPrintSchema(schema, model)
            ? `"${schema.name}".` : ''}"${table.name}" IS '${table.note.replace(/'/g, "''")}'`;
          break;
        }
        case 'column': {
          const field = model.fields[comment.fieldId];
          line += ` COLUMN ${shouldPrintSchema(schema, model)
            ? `"${schema.name}".` : ''}"${table.name}"."${field.name}" IS '${field.note.replace(/'/g, "''")}'`;
          break;
        }
        default:
          break;
      }

      line += ';\n';

      return line;
    });

    return commentArr;
  }

  static export(model) {
    const database = model.database['1'];

    const usedTableNames = new Set(Object.values(model.tables).map(table => table.name));

    const statements = database.schemaIds.reduce((prevStatements, schemaId) => {
      const schema = model.schemas[schemaId];
      const { tableIds, enumIds, refIds } = schema;

      if (shouldPrintSchema(schema, model)) {
        prevStatements.schemas.push(`CREATE SCHEMA "${schema.name}";\n`);
      }

      if (!_.isEmpty(enumIds)) {
        prevStatements.enums.push(...SpannerExporter.exportEnums(enumIds, model));
      }

      const commentNodes = _.flatten(tableIds.map((tableId) => {
        const { fieldIds, note } = model.tables[tableId];
        const fieldObjects = fieldIds
          .filter((fieldId) => model.fields[fieldId].note)
          .map((fieldId) => ({ type: 'column', fieldId, tableId }));
        return note ? [{ type: 'table', tableId }].concat(fieldObjects) : fieldObjects;
      }));
      if (!_.isEmpty(commentNodes)) {
        prevStatements.comments.push(...SpannerExporter.exportComments(commentNodes, model));
      }

      try {
        // Loop through the tables, append table and all relevant indexes to the statements
        if (!_.isEmpty(tableIds)) {
          _.flatten(tableIds.map((tableId) => {
            prevStatements.tablesAndIndexes.push(...SpannerExporter.exportTableAndIndexes(tableId, model));
          }));
        }
      } catch (e) {
        console.log("\t" + e.toString());
      }

      if (!_.isEmpty(refIds)) {
        prevStatements.refs.push(...SpannerExporter.exportRefs(refIds, model, usedTableNames));
      }

      return prevStatements;
    }, {
      schemas: [],
      enums: [],
      tablesAndIndexes: [],
      comments: [],
      refs: [],
    });


    // collate tables and indexes so that indexes appear after table. 



    const res = _.concat(
      statements.schemas,
      statements.enums,
      statements.tablesAndIndexes,
      statements.comments,
      statements.refs,
    ).join('\n');
    return res;
  }
}

export default SpannerExporter;
