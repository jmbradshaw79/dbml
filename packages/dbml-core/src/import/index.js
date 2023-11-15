import Parser from '../parse/Parser';
import ModelExporter from '../export/ModelExporter';

function _import (str, format) {
  const database = (new Parser()).parse(str, format);
  const dbml = ModelExporter.export(database.normalize(), 'dbml');

  return dbml;
}

export default {
  import: _import,
};
