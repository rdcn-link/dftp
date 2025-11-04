package link.rdcn.optree.fifo

import org.json.{JSONArray, JSONObject}

case class DockerContainer(
                            containerName: String,
                            hostPath: Option[String] = None,
                            containerPath: Option[String] = None,
                            imageName: Option[String] = None
                          ){
  def start(): String = {
    if(!DockerExecute.isContainerRunning(containerName)){
      DockerExecute.startContainer(hostPath.get, containerPath.get, containerName, imageName.get)
    } else containerName
  }

  def toJson(): JSONObject = {
    val jo = new JSONObject
    jo.put("containerName", containerName)
    hostPath.map(jo.put("hostPath", _))
    containerPath.map(jo.put("containerPath", _))
    imageName.map(jo.put("imageName", _))
    jo
  }
}

object DockerContainer{
  def fromJson(jo: JSONObject): DockerContainer = {
    DockerContainer(jo.getString("containerName"),
      Option(jo.optString("hostPath", null)),
      Option(jo.optString("containerPath", null)),
      Option(jo.optString("imageName", null))
    )
  }
}
