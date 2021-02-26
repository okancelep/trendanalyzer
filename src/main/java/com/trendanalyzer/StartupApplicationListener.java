package com.trendanalyzer;

import com.google.gson.*;
import com.trendanalyzer.model.Performance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class StartupApplicationListener implements
        ApplicationListener<ContextRefreshedEvent> {

    private final static Logger logger = LoggerFactory.getLogger(StartupApplicationListener.class);

    @Value("${threshold}")
    private String threshold;

    @Value("${spikeHeight}")
    private String spikeHeight;

    @Value("${referenceWindow}")
    private String referenceWindow;

    @Value("${minDocCount}")
    private String minDocCount;

    @Value("${metricAggKey}")
    private String metricAggKey;

    @Value("${esUrl}")
    private String esUrl;

    @Value("${indexPattern}")
    private String indexPattern;

    @Value("${curCountThreshold}")
    private String curCountThreshold;

    @Value("${curWindow}")
    private String curWindow;

    @Value("${mail.to}")
    private String mailTo;

    @Value("${mail.subject}")
    private String mailSubject;

    @Value("${mail.from}")
    private String mailFrom;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private JavaMailSender javaMailSender;

    @Value("${metricKey}")
    private String metricKey;

    @Value("${include}")
    private String include;

    @Value("${exclude}")
    private String exclude;

    @Value("${alertBody}")
    private String alertBody;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        ApplicationContext ctx = ((ContextRefreshedEvent) event).getApplicationContext();
        try{
            String spikeMsg="";
            Double spikeHeightDbl = Double.parseDouble(spikeHeight);
            Double thresholdDbl = Double.parseDouble(threshold);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            String refRequest = prepareRequest(Integer.parseInt(referenceWindow), 1, 0);
            String curRequest = prepareRequest(0, 0, Integer.parseInt(curWindow));

            HttpEntity<String> refEntity = new HttpEntity<String>(refRequest, headers);
            HttpEntity<String> curEntity = new HttpEntity<String>(curRequest, headers);

            ResponseEntity<String> refResponse = restTemplate.postForEntity(esUrl+"/"+indexPattern+"*/_search", refEntity, String.class);
            HashMap<String, Performance> refMap = extractMetricAgg(refResponse);

            ResponseEntity<String> curResponse = restTemplate.postForEntity(esUrl+"/"+indexPattern+"*/_search", curEntity, String.class);
            HashMap<String, Performance> curMap = extractMetricAgg(curResponse);

            /*Set<String> metricKeyRefSet = refMap.keySet();
            Iterator<String> itRef = metricKeyRefSet.iterator();
            while(itRef.hasNext()){
                String metricKey = itRef.next();
                Performance refPerf = refMap.get(metricKey);
                BigDecimal refAvg = refPerf.getSum().divide(new BigDecimal(String.valueOf(refPerf.getCount())), 10, RoundingMode.HALF_UP);
                System.out.println(metricKey + "\t\t-"+refAvg+"-"+refPerf.getCount());
            }*/

            String alertText = alertBody+ " This trend shows comparison to "+ referenceWindow + " days of previous performance .<br><b>Parameters:</b><br>Threshold Window > "+threshold + " seconds<br>"+"Spike Rate > "+Math.round((spikeHeightDbl-1)*100)+ "%<br>Current Request Count > "+minDocCount+ "<br>Current control window:"+curWindow + " minutes";
            String emailHtml =  "<!DOCTYPE html><html lang=\"tr\"><head>\n" +
                    "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\">\n" +
                    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                    "<body><div class=\"cerceve\" id=\"cerceve\">\n" +
                    "<p style=\"font-size:14px;font-family:Calibri,Arial,Verdana;\">"+alertText+"</p>\n" +
                    "<br>\n" +
                    "<table cellpadding=\"3\" cellspacing=\"1\" width=\"100%\" style=\"border-radius:4px;font-size:14px;font-family:Calibri,Arial,Verdana;\">\n" +
                    "    <tr style = \"background-color:#1C6EA4;color:white\">\n" +
                    "\t<td width=\"20%\">Url</td>\n" +
                    "\t<td width=\"10%\">Current Request Average Time (sn)</td>\n" +
                    "\t<td width=\"10%\">Reference Request Average Time (sn)</td>\n" +
                    "\t<td width=\"10%\">Spike Rate(%)</td>\n" +
                    "\t<td width=\"10%\">Current Request Count</td>\n" +
                    "\t<td width=\"10%\">Reference Request Count</td>\n" +
                    "    </tr>\n";
            Set<String> metricKeySet = curMap.keySet();
            Iterator<String> it = metricKeySet.iterator();
            while(it.hasNext()){
                String metricKey = it.next();
                Performance curPerf = curMap.get(metricKey);
                Performance refPerf = refMap.get(metricKey);

                if(curPerf != null & refPerf != null && curPerf.getCount() > Integer.parseInt(curCountThreshold)){
                    BigDecimal refAvg = refPerf.getSum().divide(new BigDecimal(String.valueOf(refPerf.getCount())), 3, RoundingMode.HALF_UP);
                    BigDecimal curAvg = curPerf.getSum().divide(new BigDecimal(String.valueOf(curPerf.getCount())), 3, RoundingMode.HALF_UP);
                    if(curAvg.compareTo(BigDecimal.valueOf(thresholdDbl)) == 1) {
                        if (curAvg.compareTo(refAvg.multiply(BigDecimal.valueOf(spikeHeightDbl))) == 1) {
                            BigDecimal durationChange = curAvg.divide(refAvg,4, RoundingMode.HALF_UP);
                            spikeMsg += "curAvg:"+curAvg+"\t\trefAvg:"+refAvg+"\t\tcurCnt:"+curPerf.getCount()+"\t\trefCount:"+refPerf.getCount()+"\t\tSpike:"+durationChange+"\t\t"+metricKey+"\n";

                            emailHtml += "\t<tr>\n" +
                                    "\t\t<td>"+metricKey+"</td>\n" +
                                    "\t\t<td>"+curAvg+"</td>\n" +
                                    "\t\t<td>"+refAvg+"</td>\n" +
                                    "\t\t<td>"+durationChange.subtract(BigDecimal.ONE).multiply(BigDecimal.valueOf(100L)).setScale(2)+"%</td>\n" +
                                    "\t\t<td>"+curPerf.getCount()+"</td>\n" +
                                    "\t\t<td>"+refPerf.getCount()+"</td>\n" +
                                    "\t</tr>\n";
                        }
                    }
                }
            }

            emailHtml += "</table>\n" +
                    "</div>\n" +
                    "</body>\n" +
                    "</html>";

            if(!"".equals(spikeMsg)) {
                MimeMessage message = javaMailSender.createMimeMessage();
                MimeMessageHelper helper = new MimeMessageHelper(message, true);

                helper.setTo(InternetAddress.parse(mailTo));
                helper.setSubject(mailSubject);
                helper.setText(emailHtml, true);
                helper.setFrom(mailFrom);

                javaMailSender.send(message);
                System.out.println("All crawling complete\n"+spikeMsg);
            }else
                System.out.println("All crawling complete. There is no event to alert.");

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Application failed to start",e);
            SpringApplication.exit(ctx, () -> 1);
        }

        SpringApplication.exit(ctx, () -> 0);

    }

    private String prepareRequest(int dayWindow, int hourWindow, int minuteWindow) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
        String requestJson = "{\n" +
                "    \"size\":0,\n" +
                "    \"aggs\": {\n" +
                "    \"range\": {\n" +
                "         \"date_range\": {\n" +
                "             \"field\": \"@timestamp\",\n" +
                "             \"format\": \"yyyy/MM/dd HH:mm:ss\",\n" +
                "             \"time_zone\": \"+03:00\",\n" +
                "             \"ranges\": [\n";
        for(int i=0; i<dayWindow||i==0; i++){
            Calendar startCal = new GregorianCalendar();
            if(dayWindow>0)
                startCal.add(Calendar.DATE, -i-1);
            if(hourWindow>0)
                startCal.add(Calendar.HOUR, -1*hourWindow);
            if(minuteWindow>0)
                startCal.add(Calendar.MINUTE, -1*minuteWindow);
            Date startDate = startCal.getTime();

            Calendar endCal = new GregorianCalendar();
            if(dayWindow>0)
                endCal.add(Calendar.DATE, -i-1);
            if(hourWindow>0)
                endCal.add(Calendar.HOUR, hourWindow);
            if(minuteWindow>0)
                endCal.add(Calendar.MINUTE, minuteWindow);

            Date endDate = endCal.getTime();
            requestJson += "                { \"from\": \""+dateFormat.format(startDate)+" "+timeFormat.format(startDate)+"\", \"to\" : \""+dateFormat.format(endDate)+" "+timeFormat.format(endDate)+"\" },\n";
        }
        requestJson = requestJson.substring(0, requestJson.length()-2);
        requestJson += "            ]\n" +
                "        },\n" +
                "        \"aggs\":{\n" +
                "            \"group_by_state\": {\n" +
                "                  \"terms\": {\n" +
                "                    \"field\": \""+metricKey+".keyword\",\n" +
                "                    \"size\": \"5000\",\n";
        if(include != null && !"".equals(include))
            requestJson += "                    \"include\" : \""+include+"\",\n";
        if(exclude != null && !"".equals(exclude))
            requestJson += "                    \"exclude\": \""+exclude+"\", \n";
        requestJson +=
                "                    \"order\": {\n" +
                "                      \"avg_duration\": \"desc\"\n" +
                "                    },\n" +
                "                    \"min_doc_count\" : " + Integer.parseInt(minDocCount)+
                "                  },\n" +
                "                  \"aggs\": {\n" +
                "                    \"avg_duration\": {\n" +
                "                      \"avg\": {\n" +
                "                        \"field\": \""+metricAggKey+"\"\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }              \n" +
                "        }\n" +
                "    }\n" +
                "  }\n" +
                "        \n" +
                "    \n" +
                "}";
        return requestJson;
    }


    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder.build();
    }

    private HashMap<String, Performance> extractMetricAgg(ResponseEntity<String> response) {
        Gson gson = new Gson();
        JsonParser parser = new JsonParser();
        HashMap<String, Performance> metricMap = new HashMap<String, Performance>();
        JsonArray referenceArray =  parser.parse(response.getBody()).getAsJsonObject().get("aggregations").getAsJsonObject().get("range").getAsJsonObject().get("buckets").getAsJsonArray();

        for (int i =0; i<referenceArray.size(); i++) {
            JsonElement dateArray = referenceArray.get(i);
            JsonArray urlArray = ((JsonObject) dateArray).get("group_by_state").getAsJsonObject().get("buckets").getAsJsonArray();
            for (int j=0; j<urlArray.size(); j++) {
                JsonElement urlElement = urlArray.get(j);
                String url = ((JsonObject) urlElement).get("key").getAsString();
                int docCount = ((JsonObject) urlElement).get("doc_count").getAsInt();
                BigDecimal avgDuration = ((JsonObject) urlElement).get("avg_duration").getAsJsonObject().get("value").getAsBigDecimal();

                Performance oldPerformance = metricMap.get(url);
                Performance performance = new Performance();
                int sumCount=docCount;
                BigDecimal sumDuration=avgDuration.multiply(new BigDecimal(docCount));
                if(oldPerformance != null){
                    sumCount += oldPerformance.getCount();
                    sumDuration = sumDuration.add(oldPerformance.getSum());
                }
                performance.setKey(url);
                performance.setCount(sumCount);
                performance.setSum(sumDuration);
                metricMap.put(url, performance);
            }
        }
        return metricMap;
    }
}