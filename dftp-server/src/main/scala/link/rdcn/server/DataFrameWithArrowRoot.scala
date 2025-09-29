package link.rdcn.server

import link.rdcn.struct.{ClosableIterator, DefaultDataFrame, Row, StructType}
import org.apache.arrow.vector.VectorSchemaRoot

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/26 19:56
 * @Modified By:
 */
class DataFrameWithArrowRoot(root: VectorSchemaRoot,
                             schema: StructType,
                             iter: Iterator[Row]
                            ) extends DefaultDataFrame(schema, ClosableIterator(iter)())