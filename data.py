from email import charset
import fdb
import xml.etree.ElementTree as ET
import datetime
import os

# определяем функцией выгрузку из бд
def getsql(sel):
    con = fdb.connect(dsn='10.100.0.10:/home/kom/share/PROD2.fdb', user='An1', password='0', charset='win1251')
    cur = con.cursor()
    cur = cur.execute(sel)
    return cur

#базовая функция запроса на 1 строчку, выгружает из базы указанный запрос(select) 
def get(select):
    try:
        cur = getsql(select)
        get = str(*cur.fetchone())
        return get
    except Exception:
        print("ошибка def get")

# формируем список накладных - SELECT 
SELECT = "SELECT * FROM ZEPNAR WHERE DAT_S > '2022-01-11 00:00:00.000' ORDER BY UCH"
cur = getsql(SELECT)

def get_probe(mat): # забираем пробу из базы
    try:
        SELECT = "select PROBA from IZDEL WHERE MAT = "+ "\'"+mat+"\'"
        cur = getsql(SELECT)
        proba = str(*cur.fetchone())
        return proba
    except Exception:
        print("ошибка def get_probe")

def get_forma(mat): #забираем форму вставки из бд
    try:
        zap = "select MAT from SP_VST WHERE KOD = "+ str(mat)
        cur = getsql(zap)
        forma = str(*cur.fetchone())
        return forma
    except Exception:
        print("ошибка def get_forma")

def get_vid(mat): #вытаскиваем наименование изделия по артикулу(mat - артикул)
    try:
        SEL = "SELECT MAT FROM spvid WHERE KOD = (select spvid from IZDEL WHERE mat = '"+ str(mat)+"')"
        cur = getsql(SEL)
        vid = str(*cur.fetchone())
        return vid
    except Exception:
        print("ошибка def get_vid")

def genxml(filename,uch,num):
    # формируем xml документ 
    # выгрузка всех накладных после 11-01-2022 по указанному учредителю
    # SELECT = 'SELECT ID,DAT_S,NUM_DOC,UCH FROM ZEPNAR WHERE DAT_S > \'2022-01-11 00:00:00.000\' and UCH ='+ str(uch)+" ORDER BY DAT_S"
    SELECT = 'SELECT * FROM ZEPNAR WHERE DAT_S > \'2022-01-11 00:00:00.000\' and UCH = ' + uch + ' AND  NUM_DOC = ' + num
    curl = getsql(SELECT)
    if curl.fetchone() == None:
        return print("ДАННЫХ ПО ТАКИМ НАКЛАДНЫМ НЕТ"); 
    
    curl = getsql(SELECT)
    print(SELECT)
    print("-" * 100)
    for row in curl.itermap(): #перебираем таблицу накладных
        docs = ""
        docs = ET.Element('DOCUMENTS')
        doc = "" # потребуется при разделении накладных, сейчас все выгружаются в 1 файл 
        datadoc = datetime.datetime.fromisoformat(str(row['DAT_S'])).strftime("%m.%d.%Y")
        print(str(row['ID']) + " Номер накладной: ", str(row['NUM_DOC']), datetime.datetime.fromisoformat(str(row['DAT_S'])).strftime("%m.%d.%Y"))
        doc         = ET.Element('DOCUMENT')
        docnomer    = ET.SubElement(doc, 'DOCNOMER');   docnomer.text = str(row['NUM_DOC'])
        docdata     = ET.SubElement(doc, 'DOCDATA');    docdata.text = (str(datadoc))
        select = 'select MAT,RAZMER,KOL,MASS,STOIM,NDS,ST_ALL,SCANCOD,INT_PART,YEAR_PART,DOP_PART,ST_GR from zapa WHERE idzepnar = '+ str(row['ID']) 
        cur3 = getsql(select) #делаем выборку деталей из накладной
        for row3 in cur3.itermap(): #перебираем все детали в накладной 
            trow        = ET.SubElement(doc, 'ROW')
            ARTIKUL	    = ET.SubElement(trow, 'ARTIKUL');   ARTIKUL.text = str(row3['MAT'])
            RAZMER      = ET.SubElement(trow, 'RAZMER');    RAZMER.text = str(row3['RAZMER']).replace(",", ".")
            KOLICH      = ET.SubElement(trow, 'KOLICH');    KOLICH.text = str(row3['KOL'])
            VES	        = ET.SubElement(trow, 'VES');       VES.text = str(row3['MASS'])
            CENA        = ET.SubElement(trow, 'CENA');      CENA.text = str(row3['STOIM'])
            SUMMANDS    = ET.SubElement(trow, 'SUMMANDS');  SUMMANDS.text = str(row3['NDS'])
            SUMMA       = ET.SubElement(trow, 'SUMMA');     SUMMA.text = str(row3['ST_ALL'])
            # PROBAPROBA  = ET.SubElement(trow, 'PROBAPROBA');PROBAPROBA.text = get_probe(str(row3['MAT'])) + ".000"
            # PROBAMET    = ET.SubElement(trow, 'PROBAMET');  PROBAMET.text = str(get("SELECT mat FROM SPGROUPP s WHERE KOD = (SELECT KODGR FROM SPPOD_GR sg WHERE KOD = (select GROUPP from IZDEL WHERE mat = '" + str(row3['MAT']) + "'))")).strip()
            # PROBANAME   = ET.SubElement(trow, 'PROBANAME'); PROBANAME.text = PROBAPROBA.text + " - " + PROBAMET.text 
            TOVNAME     = ET.SubElement(trow, 'TOVNAME');   TOVNAME.text = str(get_vid(str(row3['MAT']))).strip()
            TINAME      = ET.SubElement(trow, 'TINAME');    TINAME.text = TOVNAME.text
            # VESOVOY     = ET.SubElement(trow, 'VESOVOY')
            TIVES       = ET.SubElement(trow, 'TIVES')
            SNNAME      = ET.SubElement(trow, 'SNNAME');    SNNAME.text = str(row3['SCANCOD'])
            TIRAZMER    = ET.SubElement(trow, 'TIRAZMER'); TIRAZMER.text = "истина"
            if row3['RAZMER']==0:
                RAZMER.text = ""; TIRAZMER.text = "ложь"
            # if row3['INT_PART']==0:
            #     VESOVOY.text = "истина"; TIVES.text = "истина"; print("Вставок нет"); CENA.text = str(row3['STOIM'])
            # else:
            #     VESOVOY.text = "ложь"; TIVES.text = "ложь"
            if  row3['INT_PART'] != 0:
                select2 = 'SELECT INT_PART,YEAR_PART,DOP_PART,KOL,MASSK,RAZMER_VST,F_VST,GRC,GRK,SITO,MAT,DOP FROM VSTZAPA WHERE MAT2 = '+ "\'" + str(row3['MAT'])+ "\'"
                cur2 = getsql(select2) #делаем выгрузку вставок из бд по артикулу детали
                string = ""
                print("--- Вставки в " + str(TINAME.text))
                ROWXN       = ET.SubElement(trow, 'ROWXN')
                XNNAME      = ET.SubElement(ROWXN, 'XNNAME')
                XNFNAME	    = ET.SubElement(ROWXN, 'XNFNAME')
                for row2 in cur2.itermap(): #перебираем все вставки и добавляем в документ
                    if  row2['INT_PART']==row3['INT_PART'] and row2['YEAR_PART']==row3['YEAR_PART'] and row2['DOP_PART']==row3['DOP_PART']:
                        XNSKOL      = ET.SubElement(ROWXN, 'XNSKOL');       XNSKOL.text = str(row2['KOL'])
                        XNSVES      = ET.SubElement(ROWXN, 'XNSVES');       XNSVES.text = str(row2['MASSK'])
                        XNSKRASHET  = ET.SubElement(ROWXN, 'XNSKRASHET');   XNSKRASHET.text = "ложь"
                        XNSRAZMER1  = ET.SubElement(ROWXN, 'XNSRAZMER1');   XNSRAZMER1.text = str(row2['RAZMER_VST'])
                        # XNSCKGC     = ET.SubElement(ROWXN, 'XNSCKGC');      XNSCKGC.text = "истина"
                        # XNSCKGD     = ET.SubElement(ROWXN, 'XNSCKGD');      XNSCKGD.text = "истина"
                        XNSGRCVET   = ET.SubElement(ROWXN, 'XNSGRCVET');    XNSGRCVET.text = str(row2['GRC'])
                        XNSGRDEF    = ET.SubElement(ROWXN, 'XNSGRDEF');     XNSGRDEF.text = str(row2['GRK'])
                        XNSKKARAT   = ET.SubElement(ROWXN, 'XNSKKARAT');    XNSKKARAT.text = "истина"
                        XNSRASSEV   = ET.SubElement(ROWXN, 'XNSRASSEV')    
                        XNSFO       = ET.SubElement(ROWXN, 'XNSFO')
                        XNSKNAME    = ET.SubElement(ROWXN, 'XNSKNAME')
                        if str(row2['SITO']) != "0":
                            XNSRASSEV.text = str(get("SELECT mat FROM SPSITO s WHERE KOD = " + str(row2['SITO']))).strip()
                        else:
                            XNSRASSEV.text = ""
                        print("рассев = ", XNSRASSEV.text)
                        print("код формы вставки = ", row2['F_VST'])
                        if row2['F_VST'] != 0:
                            XNSFO.text = str(get_forma(str(row2['F_VST']))).strip()
                        else:
                            XNSFO.text = ""
                        if str(row2['MAT'])=='кр-57': 
                            XNSKNAME.text = 'бриллиант'; XNSFO.text = "Кр 57"
                        elif str(row2['MAT'])=='кр-17': 
                            XNSKNAME.text = 'бриллиант'; XNSFO.text = "Кр 17"
                        else: XNSKNAME.text = str(row2['MAT']).strip().replace(" н.","", 1)
                        string = string + " " + str(XNSKOL.text) + " " + str(XNSKNAME.text) + " " + str(XNSVES.text) +"Ct" + " " + str(XNSFO.text) + " " + str(row2['DOP'] + ", ")
                        print("форма вставки = " + str(XNSFO.text), XNSKNAME.text)
                    XNNAME.text = string
                    XNFNAME.text =  string
                print(string)
                print("--- Вставки заполнены " + TOVNAME.text)
            else:
                print("Вставок в " +TOVNAME.text+ " нет")
        docs.append(doc)
        # при разделении накладных сдвинуть в цикл tree и тд добавить " Накладная "+str(row['NUM_DOC'])+" "+datadoc+
        tree = ET.ElementTree(docs)
        tree.write(" Накладная "+str(row['NUM_DOC'])+" "+datadoc+" "+filename+".xml",encoding='UTF-8',xml_declaration=None, default_namespace=None, method='xml', short_empty_elements=True)
    else: 
        print("--------- Накладная готова! --------")

# Список учредителей"
UCH = { "5":"ИП Мальков",
        "6":"ИП Смирнова",
        "1":"ИП Алексеева",
        "7":"ИП Смирнов",
        "7003":"Магазин город",
        "7008":"старая деревня",
        "7013":"северное сияние",
        "7015":"магазин нарвский"}

print("________ Для справки  _________")
num = input(""" \nИП Мальков  - 5\nИП Смирнова - 6\nИП Алексеева - 1\nИП Смирнов - 7\nМагазин город - 7003\nстарая деревня - 7008\nсеверное сияние - 7013\nмагазин  нарвский - 7015\n\nВведите номер учередителя :""")
number = input("Введите номер накладной: ")

if num in UCH:
    print(UCH[num],num,number)
    try: 
        os.mkdir(UCH[num])
    except:
        print("папка существует идем дальше")
    os.chdir(UCH[num])
    if genxml(UCH[num],num,number) == None:
        print("-------- больше накладных не найдено ---------")
else:
    print("Такого учередителя нет в базе")


# print("------------Начинаем выгружать накладные ИП Смирнова -------------------")
# genxml(" ИП Смирнова.xml",6)
# print("!!!!!!!!------------ Выгружены накладные ИП Смирнова ------------- !!!!!!!!!!")
# print("-" * 100)

# print("------------Начинаем выгружать накладные ИП Смирнов -------------------")
# genxml(" ИП Смирнов.xml",7)
# print("!!!!!!!!------------ Выгружены накладные ИП Смирнов ------------- !!!!!!!!!!")

# print("------------Начинаем выгружать накладные Магазин город -------------------")
# genxml(" Магазин город.xml",7003)
# print("-" * 100)

# print("------------Начинаем выгружать накладные старая деревня -------------------")
# genxml(" старая деревня.xml",7008)
# print("-" * 100)

# print("------------Начинаем выгружать накладные северное сияние -------------------")
# genxml(" северное сияние.xml",7013)
# print("-" * 100)

# print("------------Начинаем выгружать накладные Магазин нарвский -------------------")
# genxml(" магазин нарвский.xml",7015)
# print("-" * 100)


# print(cur)
# for row in cur.itermap(): #перебираем таблицу накладных
#     if str(row['UCH'])=="5":
#     #     genxml("ИП Мальков.xml",5)
#         print("Выгружены накладные ИП Мальков")
#         print("-" * 100)
#     elif str(row['UCH'])=="6":
#         genxml(" ИП Смирнова.xml",6)
#         print("Выгружены накладные ИП Смирнова")
#         print("-" * 100)
#     # elif str(row['UCH'])=="1":
#     #     # genxml(" ИП Алексеева.xml",1)
#     #     print("Выгружены накладные ИП Алексеева")
#     #     print("-" * 100)
#     elif str(row['UCH'])=="7":
#         # genxml(" ИП Смирнов.xml",7)
#         print("Выгружены накладные ИП Смирнов")
#         print("-" * 100)
#     elif str(row['UCH'])=="7003":
#         # genxml(" Магазин город.xml",7003)
#         print("Выгружены накладные Магазин город")
#         print("-" * 100)
#     elif str(row['UCH'])=="7008":
#         # genxml(" старая деревня.xml",7008)
#         print("Выгружены накладные старая деревня")
#         print("-" * 100)
#     elif str(row['UCH'])=="7013":
#         # genxml(" северное сияние.xml",7013)
#         print("Выгружены накладные северное сияние")
#         print("-" * 100)
#     elif str(row['UCH'])=="7015":
#         # genxml(" магазин нарвский.xml",7015)
#         print("Выгружены накладные магазин нарвский")
#         print("-" * 100)

