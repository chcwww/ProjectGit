"""
更新紀錄：
2022/09/17 selenium將套件更新到4.4.3版本，因此寫法全部都更新過
2022/12/29 蝦皮API加入防爬蟲機制，因此改用seleniumwire撈取封包進行爬取
"""

import requests
import json
import pandas as pd
import time
from seleniumwire import webdriver # 需安裝：pip install selenium-wire
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import re
import random
import zlib

keyword = '花襯衫'
page = 3
ecode = 'utf-8-sig'

# 2022/11/21 由於蝦皮API更新，商品細節資料無法再單純使用request爬取，因此header只留給爬留言使用，而流言的API沒什麼檢查
my_headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    'if-none-match-': '55b03-6d83b58414a54cb5ffbe81099940f836'
    }     
# 進入每個商品，抓取買家留言
def goods_comments(item_id, shop_id):
    url = 'https://shopee.tw/api/v4/item/get_ratings?filter=0&flag=1&itemid='+ str(item_id) + '&limit=50&offset=0&shopid=' + str(shop_id) + '&type=0'
    r = requests.get(url,headers = my_headers)
    st= r.text.replace("\\n","^n")
    st=st.replace("\\t","^t")
    st=st.replace("\\r","^r")
    
    gj=json.loads(st)
    return gj['data']['ratings']
    

# 進入每個商品，抓取賣家更細節的資料（商品文案、SKU）
# https://shopee.tw/api/v4/item/get?itemid=17652103038&shopid=36023817
def goods_detail(url, item_id, shop_id):
    # 2022/12/29 ivan，因shopee API新增了防爬蟲機制，header中多了「af-ac-enc-dat」參數，因解析不出此參數如何製成，只能使用土法煉鋼，一頁一頁進去攔封包
    driver.get(url) # 需要到那個頁面，才能度過防爬蟲機制
    time.sleep(random.randint(10,20))
    getPacket = ''
    for request in driver.requests:
        if request.response:
            # 挑出商品詳細資料的json封包
            if 'https://shopee.tw/api/v4/item/get?itemid=' + str(item_id) + '&shopid=' + str(shop_id) in request.url:
                # 此封包是有壓縮的，因此需要解壓縮
                getPacket = zlib.decompress(
                    request.response.body,
                    16+zlib.MAX_WBITS
                    )
                break
    if getPacket != '':
        gj=json.loads(getPacket)
        return gj['data']
    else:
        return getPacket

# 自動下載ChromeDriver
service = ChromeService(executable_path=ChromeDriverManager().install())

# 關閉通知提醒
options = webdriver.ChromeOptions()
prefs = {"profile.default_content_setting_values.notifications" : 2}
options.add_experimental_option("prefs",prefs)
# 不載入圖片，提升爬蟲速度
options.add_argument('blink-settings=imagesEnabled=false') 

# 開啟瀏覽器
driver = webdriver.Chrome(service=service, chrome_options=options)
time.sleep(random.randint(5,10))

# 開啟網頁
driver.get('https://shopee.tw/search?keyword=' + keyword )
time.sleep(random.randint(10,20))


print('---------- 開始進行爬蟲 ----------')
tStart = time.time()#計時開始

container_product = pd.DataFrame()
container_comment = pd.DataFrame()
for i in range(int(page)):
    # 準備用來存放資料的陣列
    itemid = []
    shopid =[]
    name = []
    link = []
    brand = []
    stock = []
    price = []
    ctime = []
    description = []
    discount = []
    can_use_bundle_deal = []
    can_use_wholesale = []
    tier_variations = []
    historical_sold = []
    is_cc_installment_payment_eligible = []
    is_official_shop = []
    is_pre_order = []
    liked_count = []
    shop_location = []
    SKU = []
    cmt_count = []
    five_star = []
    four_star = []
    three_star = []
    two_star = []
    one_star = []
    rating_star = []
    
    driver.get('https://shopee.tw/search?keyword=' + keyword + '&page=' + str(i))
    time.sleep(random.randint(5,10))
    # 滾動頁面
    for scroll in range(6):
        driver.execute_script('window.scrollBy(0,1000)')
        time.sleep(random.randint(3,10))
    
    #取得商品內容
    for item, thename in zip(driver.find_elements(by=By.XPATH, value='//*[@data-sqe="link"]'), driver.find_elements(by=By.XPATH, value='//*[@data-sqe="name"]')):
        #商品ID、商家ID、商品連結
        getID = item.get_attribute('href')
        theitemid = int((getID[getID.rfind('.')+1:getID.rfind('?')]))
        theshopid = int(getID[ getID[:getID.rfind('.')].rfind('.')+1 :getID.rfind('.')]) 
        link.append(getID)
        itemid.append(theitemid)
        shopid.append(theshopid)
        
        #商品名稱
        getname = thename.text.split('\n')[0]
        name.append(getname)
        
        #價格
        thecontent = item.text
        thecontent = thecontent[(thecontent.find(getname)) + len(getname):]
        thecontent = thecontent.replace('萬','000')
        thecut = thecontent.split('\n')

        if bool(re.search('市|區|縣|鄉|海外|中國大陸', thecontent)): #有時會沒有商品地點資料
            if bool(re.search('已售出', thecontent)): #有時會沒銷售資料
                if '出售' in thecut[-3][1:]:
                    theprice = thecut[-4][1:]
                else:
                    theprice = thecut[-3][1:]

            else:
                theprice = thecut[-2][1:]
        else:
            if re.search('已售出', thecontent): #有時會沒銷售資料
                theprice = thecut[-2][1:]
            else:
                theprice = thecut[-1][1:]
                
        theprice = theprice.replace('$','')
        theprice = theprice.replace(',','')
        theprice = theprice.replace('售','')
        theprice = theprice.replace('出','')
        theprice = theprice.replace(' ','')
        if ' - ' in theprice:
            theprice = (int(theprice.split(' - ')[0]) +int(theprice.split(' - ')[1]))/2
        if '-' in theprice:
            theprice = (int(theprice.split('-')[0]) +int(theprice.split('-')[1]))/2
        price.append(int(theprice))

    for lnk, itid, shid in zip(link, itemid, shopid):
        #請求商品詳細資料
        itemDetail = goods_detail(url = lnk, item_id = itid, shop_id = shid)
        print('抓取： '+ str(itid) )
        if itemDetail == '':
            # 抓不到資料就全部塞空值
            print('抓不到商品詳細資料...\n')
            brand.append(None) #品牌
            stock.append(None) #存貨數量
            ctime.append(None) #上架時間
            description.append(None) #商品文案
            discount.append(None) #折數
            can_use_bundle_deal.append(None) #可否搭配購買
            can_use_wholesale.append(None) #可否大量批貨購買
            tier_variations.append(None) #選項
            historical_sold.append(None) #歷史銷售量
            is_cc_installment_payment_eligible.append(None) #可否分期付款
            is_official_shop.append(None) #是否官方賣家帳號
            is_pre_order.append(None) #是否可預購
            liked_count.append(None) #喜愛數量
            SKU.append(None) #SKU
            shop_location.append(None) #商家地點
            cmt_count.append(None) #評價數量
            five_star.append(None) #五星
            four_star.append(None) #四星
            three_star.append(None) #三星
            two_star.append(None) #二星
            one_star.append(None) #一星
            rating_star.append(None) #評分
            continue

        brand.append(itemDetail['brand']) #品牌
        stock.append(itemDetail['stock']) #存貨數量
        ctime.append(itemDetail['ctime']) #上架時間
        description.append(itemDetail['description']) #商品文案
        discount.append(itemDetail['discount']) #折數
        can_use_bundle_deal.append(itemDetail['can_use_bundle_deal']) #可否搭配購買
        can_use_wholesale.append(itemDetail['can_use_wholesale']) #可否大量批貨購買
        tier_variations.append(itemDetail['tier_variations']) #選項
        historical_sold.append(itemDetail['historical_sold']) #歷史銷售量
        is_cc_installment_payment_eligible.append(itemDetail['is_cc_installment_payment_eligible']) #可否分期付款
        is_official_shop.append(itemDetail['is_official_shop']) #是否官方賣家帳號
        is_pre_order.append(itemDetail['is_pre_order']) #是否可預購
        liked_count.append(itemDetail['liked_count']) #喜愛數量
        
        #SKU
        all_sku=[]
        for sk in itemDetail['models']:
            all_sku.append(sk['name'])
        SKU.append(all_sku) #SKU
        shop_location.append(itemDetail['shop_location']) #商家地點
        cmt_count.append(itemDetail['cmt_count']) #評價數量
        five_star.append( itemDetail['item_rating']['rating_count'][5] ) #五星
        four_star.append( itemDetail['item_rating']['rating_count'][4] ) #四星
        three_star.append( itemDetail['item_rating']['rating_count'][3] ) #三星
        two_star.append( itemDetail['item_rating']['rating_count'][2] ) #二星
        one_star.append( itemDetail['item_rating']['rating_count'][1] ) #一星
        rating_star.append(itemDetail['item_rating']['rating_star']) #評分
        
        # 消費者評論詳細資料
        iteComment = goods_comments(item_id = theitemid, shop_id = theshopid)
        userid = [] #使用者ID
        anonymous = [] #是否匿名
        commentTime = [] #留言時間
        is_hidden = [] #是否隱藏
        orderid = [] #訂單編號
        comment_rating_star = [] #給星
        comment = [] #留言內容
        product_SKU = [] #商品規格
        
        for comm in iteComment:
            userid.append(comm['userid'])
            anonymous.append(comm['anonymous'])
            commentTime.append(comm['ctime'])
            is_hidden.append(comm['is_hidden'])
            orderid.append(comm['orderid'])
            comment_rating_star.append(comm['rating_star'])
            try:
                comment.append(comm['comment'])
            except:
                comment.append(None)
            
            p=[]
            for pro in comm['product_items']:
                try:
                    p.append(pro['model_name'])
                except:
                    p.append(None)
                    
            product_SKU.append(p)
            
        commDic = {
            '商品ID':[ theitemid for x in range(len(iteComment)) ],
            '賣家ID':[ theshopid for x in range(len(iteComment)) ],
            '商品名稱':[ getname for x in range(len(iteComment)) ],
            '價格':[ int(theprice) for x in range(len(iteComment)) ],
            '使用者ID':userid,
            '是否匿名':anonymous,
            '留言時間':commentTime,
            '是否隱藏':is_hidden,
            '訂單編號':orderid,
            '給星':comment_rating_star,
            '留言內容':comment,
            '商品規格':product_SKU
            }

        container_comment = pd.concat([container_comment,pd.DataFrame(commDic)], axis=0)
        time.sleep(random.randint(20,150)) # 休息久一點
        
    dic = {
            '商品ID':itemid,
            '賣家ID':shopid,
            '商品名稱':name,
            '商品連結':link,
            '品牌':brand,
            '存貨數量':stock,
            '價格':price,
            '商品文案':description,
            '折數':discount,
            '可否搭配購買':can_use_bundle_deal,
            '可否大量批貨購買':can_use_wholesale,
            '選項':tier_variations,
            '歷史銷售量':historical_sold,
            '可否分期付款':is_cc_installment_payment_eligible,
            '是否官方賣家帳號':is_official_shop,
            '是否可預購':is_pre_order,
            '喜愛數量':liked_count,
            '商家地點':shop_location,
            'SKU':SKU,
            '評價數量':cmt_count,
            '五星':five_star,
            '四星':four_star,
            '三星':three_star,
            '二星':two_star,
            '一星':one_star,
            '評分':rating_star,
            }

    #資料整合
    container_product = pd.concat([container_product,pd.DataFrame(dic)], axis=0)
    #暫時存檔紀錄
    container_product.to_csv('shopeeAPIData'+str(i+1)+'_Product.csv', encoding = ecode, index=False)
    container_comment.to_csv('shopeeAPIData'+str(i+1)+'_Comment.csv', encoding = ecode, index=False)

    print('目前累積商品： ' + str(len(container_product)) + ' 留言累積' + str(len(container_comment)))
    time.sleep(random.randint(20,150)) # 休息久一點

container_product.to_csv(keyword +'_商品資料.csv', encoding = ecode, index=False)
container_comment.to_csv(keyword +'_留言資料.csv', encoding = ecode, index=False)

tEnd = time.time()#計時結束
totalTime = int(tEnd - tStart)
minute = totalTime // 60
second = totalTime % 60
print('資料儲存完成，花費時間（約）： ' + str(minute) + ' 分 ' + str(second) + '秒')

driver.close() 