package com.epam.bigdata.entity;

import java.util.Date;

/**
 * Created by Ilya_Starushchanka on 11/1/2016.
 */
public class LogsEntity {

   // private String bidId;
    private long timestamp;
    /*private String timestamp;
    private String ipinyouId;*/
    //private String userAgent;
    /*private String ip;
    private int region;
    private int city;
    private int adExchange;
    private String domain;
    private String url;
    private String anonymousUrl;
    private String adSlotId;
    private int adSlotWirdth;
    private int adSlotHeight;
    private int adSlotVisibility;
    private int adSlotFormat;*/
    private int payingPrice;
    //private String creativeId;
    private int biddingPrice;
    /*private int advertiserId;
    private long  userTags;
    private int streamId;*/
    private String device;
    private String osName;
    private String uaFamily;

    public LogsEntity(){}

    public LogsEntity(String line){
        String[] fields = line.split("\\t");
        //this.bidId = fields[0];
        this.timestamp = new Date().getTime();
       // this.timestamp = fields[1];
        //this.ipinyouId = fields[2];
        //this.userAgent = fields[3];
        //this.ip = fields[4];
       // this.region = Integer.parseInt(fields[5]);
        //this.city = Integer.parseInt(fields[6]);
       // this.adExchange = Integer.parseInt(fields[7]);
        //this.domain = fields[8];
        //this.url = fields[9];
        //this.anonymousUrl = fields[10];
        //this.adSlotId = fields[11];
        //this.adSlotWirdth = Integer.parseInt(fields[12]);
        //this.adSlotHeight = Integer.parseInt(fields[13]);
        //this.adSlotVisibility = Integer.parseInt(fields[14]);
        //this.adSlotFormat =Integer.parseInt( fields[15]);
        this.payingPrice = Integer.parseInt(fields[16]);
        //this.creativeId = fields[17];
        this.biddingPrice = Integer.parseInt(fields[18]);
        //this.advertiserId = Integer.parseInt(fields[19]);
        //this.userTags = Long.parseLong(fields[20]);
        //this.streamId = Integer.parseInt(fields[21]);
    }


    /*public String getBidId() {
        return bidId;
    }

    public void setBidId(String bidId) {
        this.bidId = bidId;
    }*/
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /*public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }*/

    /*public String getIpinyouId() {
        return ipinyouId;
    }

    public void setIpinyouId(String ipinyouId) {
        this.ipinyouId = ipinyouId;
    }*/

    /*public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }*/

    /*public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getRegion() {
        return region;
    }

    public void setRegion(int region) {
        this.region = region;
    }

    public int getCity() {
        return city;
    }

    public void setCity(int city) {
        this.city = city;
    }

    public int getAdExchange() {
        return adExchange;
    }

    public void setAdExchange(int adExchange) {
        this.adExchange = adExchange;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAnonymousUrl() {
        return anonymousUrl;
    }

    public void setAnonymousUrl(String anonymousUrl) {
        this.anonymousUrl = anonymousUrl;
    }

    public String getAdSlotId() {
        return adSlotId;
    }

    public void setAdSlotId(String adSlotId) {
        this.adSlotId = adSlotId;
    }

    public int getAdSlotWirdth() {
        return adSlotWirdth;
    }

    public void setAdSlotWirdth(int adSlotWirdth) {
        this.adSlotWirdth = adSlotWirdth;
    }

    public int getAdSlotHeight() {
        return adSlotHeight;
    }

    public void setAdSlotHeight(int adSlotHeight) {
        this.adSlotHeight = adSlotHeight;
    }

    public int getAdSlotVisibility() {
        return adSlotVisibility;
    }

    public void setAdSlotVisibility(int adSlotVisibility) {
        this.adSlotVisibility = adSlotVisibility;
    }

    public int getAdSlotFormat() {
        return adSlotFormat;
    }

    public void setAdSlotFormat(int adSlotFormat) {
        this.adSlotFormat = adSlotFormat;
    }*/

    public int getPayingPrice() {
        return payingPrice;
    }

    public void setPayingPrice(int payingPrice) {
        this.payingPrice = payingPrice;
    }

    /*public String getCreativeId() {
        return creativeId;
    }

    public void setCreativeId(String creativeId) {
        this.creativeId = creativeId;
    }*/

    public int getBiddingPrice() {
        return biddingPrice;
    }

    public void setBiddingPrice(int biddingPrice) {
        this.biddingPrice = biddingPrice;
    }

    /*public int getAdvertiserId() {
        return advertiserId;
    }

    public void setAdvertiserId(int advertiserId) {
        this.advertiserId = advertiserId;
    }

    public long getUserTags() {
        return userTags;
    }

    public void setUserTags(long userTags) {
        this.userTags = userTags;
    }

    public int getStreamId() {
        return streamId;
    }

    public void setStreamId(int streamId) {
        this.streamId = streamId;
    }*/

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getUaFamily() {
        return uaFamily;
    }

    public void setUaFamily(String uaFamily) {
        this.uaFamily = uaFamily;
    }
}
