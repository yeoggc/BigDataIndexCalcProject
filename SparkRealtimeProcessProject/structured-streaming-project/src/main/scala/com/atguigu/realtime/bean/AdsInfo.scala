package com.atguigu.realtime.bean

import java.sql.Timestamp

case class AdsInfo(ts: Long, //数字型的时间戳
                   timestamp: Timestamp,//时间戳类型的时间戳
                   dayString: String,
                   hmString: String,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String)
