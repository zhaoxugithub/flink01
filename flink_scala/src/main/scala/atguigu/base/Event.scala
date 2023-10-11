package atguigu.base


class Event(user: String, url: String, timestamp: Long) {
  var user1: String = user
  var timestamp1 = timestamp

  override def toString: String = s"[user=$user,url=$url,timestamp=$timestamp]"
}
