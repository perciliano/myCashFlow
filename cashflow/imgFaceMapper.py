#' title......: IMG face dectect and map
#' description: Reconhecimento facial e cadastro de rostos para uid
#' file.......: imgFaceMapper.py
#' version....: 0.1.3
#' author.....: Carlos Perciliano Gaudencio
#' date.......: 2023-04-07
#' update.....: 2023-04-19
#' library....: re, os, pandas, sys, multiprocessing, itertools, PIL, face_recognition, numpy
#' sample.....: from imgFaceMapper import People ; 
#'  conhecidos=People(knownfaces='knownpeople',unknownimages='/home/perciliano/Pictures/20230219_111010.jpg') ;
#'  conhecidos.writeXls(); conhecidos=People(knownfaces='knownpeople',unknownimages='/home/perciliano/Pictures',upsample=1,model='hog',saveunknowns=1,applylandmarks=True)
#' https://face-recognition.readthedocs.io  [para calibrar a precisao usar: upsample, model, tolerance]
#' https://github.com/ageitgey/face_recognition [para ver calibracao usar: saveunknowns, applylandmarks, landmarksmodel]
#' https://stackoverflow.com/questions/48235743/ubuntu-python-unable-to-pip-install-dlib-failed-building-wheel-for-dlib-and-m
#'  saveunknowns: 0.nao salva, 1.apenas face, 2.imagem reduzida mapeada, 3.imagem original mapeada [muito lento]
#'  applylandmarks: False-Nao desenha limites da face, True-Desenha
#'  optionaldf: False-Aborta com erro se nao encontrar nomes, True-Segue pelo nome do arquivo
#'  model: hog-mais leve e menos sensivel, cnn-melhor porem mais demorado [padrao]
#'  cpus: 0.padrao 60% cpus, 1.uma cpu, n.desejado ou 80% se ultrapassar, -1.todas
#'  upsample: 1.padrao, 0-100.quanto maior o nr menor e a face detectada
#'  landmarksmodel: small-rapido com menos marcacoes, large-com todas marcacoes
#'  tolerance: 0.5-padrao, 0-1.maior aceita mais diferencas podendo pegar falsos conhecidos
#import time
class People():
    def __init__(self,knownfaces='knownpeople',unknownimages=None,\
        saveunknowns=0,applylandmarks=False,optionaldf=True,model='cnn',\
        cpus=0,upsample=0,landmarksmodel='large',tolerance=0.5):
        #self.start_time=time.time()
        self.knownfaces=knownfaces \
            if (type(knownfaces)==str and len(knownfaces)>0) else None
        assert (self.knownfaces is not None),\
           'Please inform a valid string at knownfaces! Not valid dir or image.'
        self.unknownimages=unknownimages \
            if (type(unknownimages)==str and len(unknownimages)>0) else None
        assert (self.unknownimages is not None),\
           'Please inform a valid string at unknownimages! Not valid dir or image.'
        self.saveunknowns=saveunknowns if (saveunknowns in [0,1,2,3]) else 0
        self.applylandmarks=applylandmarks if \
            (self.saveunknowns>0 and applylandmarks==True) else False
        self.optionaldf=optionaldf if (optionaldf==True) else False
        self.model=model if (model in ['hog','cnn']) else 'hog'
        self.upsample=upsample if (upsample in range(0,100)) else 1
        self.landmarksmodel=landmarksmodel if (landmarksmodel in ['small','large']) \
            else 'small'
        self.tolerance=tolerance if (type(tolerance)==float and tolerance>-1) else 0.5
        #print("1:--> %s sec." % (time.time() - self.start_time))
        self.__knownSource__()
        self.__unknownTarget__()
        self.cpus=self.__fitcpus__(int(cpus))
        self.__readXLS__()
        #print("2:--> %s sec." % (time.time() - self.start_time))
        self.__startDetect__()
        #print("3:--> %s sec." % (time.time() - self.start_time))
    def __fitcpus__(self,_cpus):
        import sys
        try:
            if (sys.version_info<(3,4)) and _cpus!=1:
                return 1 #only python 3.4 greater support Multi-processing
            if (self.targetFilelen<3 and _cpus>-1):
                return 1 #poucas analises nao demandam multicore 
                #e gera erro cannot pickle '_io.BufferedReader' object
        except Exception as e:
            return 1
        from util.cpu import Available
        cpus=Available()
        try:
            if (_cpus==0):
                return int(cpus.count*0.6)
            elif (int(cpus.count)<_cpus):
                return int(cpus.count*0.8)
            elif (_cpus<0):
                return -1
            else:
                return _cpus
        except Exception as e:
            return int(cpus.count*0.5)
    def __knownSource__(self):
        import os, re
        WORKFILE="imgFaceMapData.xlsx"
        if (self.knownfaces.find(r':')>-1): #windrive
           self.knownDrive,self.knowDir=self.knownfaces.split(':')
        self.knownDir=os.path.abspath(self.knownfaces) \
            if (os.path.isdir(self.knownfaces)) else None
        self.knownFiles=[self.knownfaces] if (os.path.isfile(self.knownfaces)) else None
        if (self.knownFiles is not None):
            assert (self.knownFiles[0].lower().endswith(('.png','.jpg','.jpeg'))),\
               'Please only image type:[jpg,jpeg,png] are supported! Invalid file:{}.'.\
               format(self.knownFiles)
            self.knownFilelen=1
            self.knownDir=os.path.dirname(os.path.abspath(self.knownFiles[0]))
        elif (self.knownDir is not None):
            self.knownFiles=[os.path.join(self.knownDir,f) \
                for f in os.listdir(self.knownDir) if \
                re.match(r'.*\.(jpg|jpeg|png)',f,flags=re.IGNORECASE)]
            self.knownFilelen=len(self.knownFiles)
        else:
            raise Exception('Please inform a valid string at knownfaces!'+
                ' Not valid dir or image {}.'.format(self.knownfaces))
        assert (self.knownFilelen>0),\
            'Please only image type:[jpg,jpeg,png] are supported! Empty dir:{},{}.'.\
                format(self.knownDir,self.knownFilelen)
        self.unknownpeople=('/'.join(self.knownDir.split('/')[0:-1]))+'/unknownpeople'
        if not os.path.exists(self.unknownpeople):
            os.makedirs(self.unknownpeople)
            print('New faces directory:[',self.unknownpeople,'] created!')
        self.peopleXLS=self.knownDir+('/' if (self.knownDir.endswith('/')==False) \
            else '')+WORKFILE
    def __unknownTarget__(self):
        import os, re
        if (self.unknownimages.find(r':')>-1): #windrive
           self.targetDrive,self.targetDir=self.targetImages.split(':')
        self.targetDir=os.path.abspath(self.unknownimages) \
            if (os.path.isdir(self.unknownimages)) else None
        self.targetFiles=[self.unknownimages] if (os.path.isfile(self.unknownimages)) else None
        if (self.targetFiles is not None):
            assert (self.targetFiles[0].lower().endswith(('.png','.jpg','.jpeg'))),\
               'Please only image type:[jpg,jpeg,png] are supported! Invalid file:{}.'.\
               format(self.targetFiles)
            self.targetFilelen=1
            self.targetDir=os.path.dirname(os.path.abspath(self.targetFiles[0]))
        elif (self.targetDir is not None):
            self.targetFiles=[os.path.join(self.targetDir,f) \
                for f in os.listdir(self.targetDir) if \
                re.match(r'.*\.(jpg|jpeg|png)',f,flags=re.IGNORECASE)]
            self.targetFilelen=len(self.targetFiles)
        else:
            raise Exception('Please inform a valid string at unknownimages!'+
                ' Not valid dir or image {}.'.format(self.unknownimages))
        assert (self.targetFilelen>0),\
            'Please only image type:[jpg,jpeg,png] are supported! Empty dir:{},{}.'.\
                format(self.targetDir,self.targetFilelen)
    def __readXLS__(self):
        import os.path
        from pandas import ExcelFile, DataFrame
        tmpExist=False
        tmpExist=os.path.isfile(self.peopleXLS)
        if (tmpExist==False):
            if (self.optionaldf==True):
                self.peopledf=DataFrame(index=[], columns=['id','known','image',\
                    'suffix','first_name','last_name','middle_name','gen_suffix',\
                    'birthdate','cellphone','email']).astype({'id':int,'known':str,'image':str})
            else:
                raise Exception('Known people file:['+self.peopleXLS+'] not available!')
        else:
            self.dataXLS=ExcelFile(self.peopleXLS)
            if not 'Sheet1' in self.dataXLS.sheet_names:
                raise Exception('PeopleMap sheet data not available! [Sheet1]')
            tmpData=self.dataXLS.parse('Sheet1',index_col=None,na_values=['NA',''],\
                usecols="B:L",converters={'id':int,'known':str})
            self.peopledf=tmpData
    def __images_in_pool__(self,images_to_check,known_names,known_face_encodings,tmpReport):
        import multiprocessing
        import itertools
        from pandas import concat
        processes=None if (self.cpus==-1) else self.cpus
        # macOS will crash due to a bug in libdispatch if you don't use 'forkserver'
        context=multiprocessing
        if "forkserver" in multiprocessing.get_all_start_methods():
            context = multiprocessing.get_context("forkserver")
        pool=context.Pool(processes=processes)
        function_parameters=zip(
            range(1,len(images_to_check)+1),
            images_to_check,
            itertools.repeat(known_names),
            itertools.repeat(known_face_encodings),
            itertools.repeat(tmpReport)
        )
        tmpReport3=pool.starmap(self.__test_image__,function_parameters)
        return tmpReport3
    def __test_image__(self,imgid,image_to_check,known_names,known_face_encodings,tmpReport):
        from pandas import DataFrame
        import face_recognition, os
        from PIL import Image, ImageDraw
        import numpy as np
        from datetime import datetime
        unknownimage=image_to_check
        unknown_image=face_recognition.load_image_file(unknownimage)
        # Scale down image if it's giant so things run a little faster
        if (self.saveunknowns<3 and max(unknown_image.shape)>1600):
            pil_img=Image.fromarray(unknown_image,mode='RGB')
            pil_img.thumbnail((1600, 1600), Image.LANCZOS) #small
            unknown_image=np.array(pil_img)
        if (self.applylandmarks==True or self.saveunknowns in [2,3]):
            pil_lmarks=pil_img.copy() if ('pil_img' in locals()) \
                else Image.fromarray(unknown_image,mode='RGB')
            drwImg=ImageDraw.Draw(pil_lmarks,'RGBA')
        basename=os.path.splitext(os.path.basename(unknownimage))[0]
        #print("2.1a:--> %s sec." % (time.time() - self.start_time))
        #model=cnn:bether and slow,hog:faster, upsample=higher read smaller faces - maisrapido: model='hog'
        unknown_face_locations=face_recognition.face_locations(unknown_image,\
            self.upsample,model=self.model)
        #num_jitters=1 higher is slow, model=small is faster
        #print("2.1b:--> %s sec." % (time.time() - self.start_time))
        unknown_encodings=face_recognition.face_encodings(unknown_image,\
            unknown_face_locations,model=self.landmarksmodel)
        if (self.applylandmarks==True):
            face_landmarks_list=face_recognition.face_landmarks(unknown_image,\
                face_locations=unknown_face_locations,model=self.landmarksmodel)
            #print(len(face_landmarks_list),len(unknown_encodings),len(unknown_face_locations))
        msg=''
        distance=None
        probability=None
        name=basename
        faceimgs=[]
        i=0 #inicia em zero para uso em lenloop
        #print("2.2:--> %s sec." % (time.time() - self.start_time))
        for unknown_encoding in unknown_encodings:
            distances=face_recognition.face_distance(known_face_encodings,unknown_encoding)
            result=list(distances<=self.tolerance)
            msg='unknown_person'
            distance=None
            probability=None
            name=basename
            if True in result:
                msg=[[name,distance] \
                    for is_match,name,distance in zip(result,known_names,distances) if is_match]
                distance=min(msg)[1] #menor distancia encotrada
                name=min(msg)[0]
                probability=(1-distance)*100
            if (self.applylandmarks==True):
                face_landmarks=face_landmarks_list[i]
                #nose and chin
                drwImg.polygon(face_landmarks['chin'],fill=(68,54,39,128),width=5)
                drwImg.polygon(face_landmarks['nose_bridge'],fill=(68,54,39,128),width=5)
                drwImg.polygon(face_landmarks['nose_tip'],fill=(68,54,39,128),width=5)
                # Make the eyebrows into a nightmare
                drwImg.polygon(face_landmarks['left_eyebrow'],fill=(68,54,39,128))
                drwImg.polygon(face_landmarks['right_eyebrow'],fill=(68,54,39,128))
                drwImg.line(face_landmarks['left_eyebrow'],fill=(68,54,39,150),width=5)
                drwImg.line(face_landmarks['right_eyebrow'],fill=(68,54,39,150),width=5)
                # Gloss the lips
                drwImg.polygon(face_landmarks['top_lip'],fill=(150,0,0,128))
                drwImg.polygon(face_landmarks['bottom_lip'],fill=(150,0,0,128))
                drwImg.line(face_landmarks['top_lip'],fill=(150,0,0,64),width=8)
                drwImg.line(face_landmarks['bottom_lip'],fill=(150,0,0,64),width=8)
                # Sparkle the eyes
                drwImg.polygon(face_landmarks['left_eye'],fill=(255,255,255,30))
                drwImg.polygon(face_landmarks['right_eye'],fill=(255,255,255,30))
                # Apply some eyeliner
                drwImg.line(face_landmarks['left_eye']+\
                    [face_landmarks['left_eye'][0]],fill=(0,0,0,110),width=6)
                drwImg.line(face_landmarks['right_eye']+\
                    [face_landmarks['right_eye'][0]],fill=(0,0,0,110),width=6)
            #applylandmarks:end
            tmpid=round((imgid+(i/1000 if i>0 else 0))*1000) #round devido a erro arred int
            if (self.saveunknowns in [1,2,3]):
                im_top,im_right,im_bottom,im_left=unknown_face_locations[i]
                if (self.saveunknowns==1):
                    face_img=unknown_image[im_top:im_bottom,im_left:im_right]
                    pil_img=Image.fromarray(face_img) #pil_img.show()
                    saveFaceName=self.unknownpeople+'/f'+\
                        datetime.utcnow().strftime('%Y%m%d_%H%M%S%f_')+\
                        str(tmpid)+'.jpg'
                    faceimgs.append(saveFaceName)
                    pil_img.save(saveFaceName,"JPEG",quality=80,optimize=True,progressive=True)
                else: #2-3
                    tx_name=name+(' ['+str(round(probability,1))+'%]' if distance is not None else '')
                    text_width=drwImg.textlength(tx_name)
                    _,tx_top,_,tx_bottom=drwImg.textbbox((20,20),tx_name) #area texto
                    text_height=tx_bottom-tx_top
                    drwImg.rectangle(((im_left, im_top),(im_right,im_bottom)),outline=(0,0,255))
                    drwImg.rectangle(((im_left,im_bottom-text_height-10),\
                       (im_right,im_bottom)),fill=(0,0,255),outline=(0,0,255))
                    drwImg.text((im_left+6,im_bottom-text_height-5),tx_name,fill=(255,255,255,255))
            #saveunknowns:end
            tmpReport.loc[tmpid]=[tmpid,False,self.model,self.upsample,name,unknownimage,\
                unknown_image,unknown_encodings,unknown_face_locations,len(unknown_encodings),\
                self.tolerance,distance,probability,msg,faceimgs]
            #print("2.3:--> %s sec." % (time.time() - self.start_time))
            i+=1
        #for:unknown_encoding
        if ((self.saveunknowns>0 and self.applylandmarks==True) or (self.saveunknowns>1)):
            saveImgName=self.unknownpeople+'/lmark'+\
                datetime.utcnow().strftime('%Y%m%d_%H%M%S%f')+'.jpg'
            pil_lmarks.save(saveImgName,"JPEG",quality=80,optimize=True,progressive=True)
        if not unknown_encodings: #if (len(unknown_encodings)==0):
            msg='WARNING: No faces found in {}. Ignoring file.'.format(unknownimage)
            tmpid=int((imgid)*1000)
            tmpReport.loc[tmpid]=[tmpid,False,self.model,self.upsample,name,unknownimage,\
                unknown_image,unknown_encodings,unknown_face_locations,len(unknown_encodings),\
                self.tolerance,distance,probability,msg,None]
        if ('pil_img' in locals()):
            del pil_img
        if ('pil_lmarks' in locals()):
            del pil_lmarks
        if ('drwImg' in locals()):
            del drwImg
        tmpReport=tmpReport.reset_index(drop=True)
        return tmpReport
    def __startDetect__(self):
        from pandas import DataFrame, concat
        import face_recognition, os, re
        tmpLine=0
        tmpReport=DataFrame(index=[],columns=['id','known','model','upsample',\
            'basename','image_path','image_map','encodings','locations','totfaces',\
            'tolerance','distance','probability','msg','faceimgs']).\
            astype({'id':int,'known':bool,'upsample':int,'image_path':str,\
            'totfaces':int,'encodings':int,'distance':float,'probability':float})
        #known-faces------------
        known_names=[]
        known_face_encodings=[]
        for knownface in self.knownFiles:
            basename=os.path.splitext(os.path.basename(knownface))[0]
            img=face_recognition.load_image_file(knownface)
            encodings=face_recognition.face_encodings(img)
            msg=''
            if (len(encodings)>1):
                msg='WARNING: More than one face found in {}. Only considering the first face.'.\
                    format(knownface)
            elif (len(encodings)==0):
                msg='WARNING: No faces found in {}. Ignoring file.'.format(knownface)
            else:
                try:
                    fidImg=int(basename)
                except Exception as e:
                    fidImg=None
                    pass
                if (len(self.peopledf.loc[self.peopledf.id==fidImg].id)>0):
                    midx=min(self.peopledf.loc[self.peopledf.id==fidImg].index)
                    known_names.append(re.sub(r' +',' ',' '.join(map(str,[x for x in \
                        self.peopledf.loc[midx,['suffix','first_name','middle_name',\
                        'last_name','gen_suffix']].values if x==x]))))
                else:
                    known_names.append(basename)
                known_face_encodings.append(encodings[0])
            #logDF adding line faster with loc
            tmpReport.loc[tmpLine]=[tmpLine,True,self.model,self.upsample,basename,knownface,\
                img,encodings,[],len(encodings),self.tolerance,None,None,msg,None]
            tmpLine+=1
        tmpReport.reset_index(drop=True)
        tmpReport2=tmpReport.loc[tmpReport.id==None].copy()
        #unknown-faces----------see:__test_image__
        #print("2.1:--> %s sec." % (time.time() - self.start_time))
        if (self.cpus==1):
            tmpReport2=[self.__test_image__(imID,imFile,\
                known_names,known_face_encodings,tmpReport2.copy()) for imID,imFile \
                in zip(range(1,len(self.targetFiles)+1),self.targetFiles)]
        else:
            tmpReport2=self.__images_in_pool__(self.targetFiles,\
                known_names,known_face_encodings,tmpReport2.copy())
        tmpReport2=concat(tmpReport2,ignore_index=False) \
            if (type(tmpReport2)==list and len(tmpReport2)>1) \
            else tmpReport2[0] if (type(tmpReport2)==list) else tmpReport2
        tmpReport2=tmpReport2.reset_index(drop=True)
        tmpReport=concat([tmpReport,tmpReport2],ignore_index=True).\
            drop_duplicates(subset='id',keep="first")
        tmpReport.reset_index(drop=True)
        self.reportdf=tmpReport
        print('Finish with source dataframe:[',self.peopledf.shape,\
            '], returned dataframe:[',self.reportdf.shape,'].')
    def writeXls(self):
        from pandas import DataFrame
        assert isinstance(self.reportdf, DataFrame),'writeXls: parameter dataframe={} not a Pandas DataFrame'.\
            format(self.reportdf)
        self.reportdf.to_excel(self.unknownpeople+'/reportFaces.xlsx')
        print('File:[',self.unknownpeople+'/reportFaces.xlsx,] exported!')
##detector de faces da opencv (ha outros modelos na pasta)
##https://pypi.org/project/opencv-contrib-python/
##https://www.geeksforgeeks.org/face-detection-using-cascade-classifier-using-opencv-python/
#import cv2 #cv2.__version__
#imgpath='/home/perciliano/Pictures/20230219_111010.jpg'
#img=cv2.imread(imgpath)
#img_ratio,img_dim,_=img.shape
#img_dim=(int(img_dim*(500.0/img_ratio)),500) #redimenciona para 500px de dimesao na proporcao
#img_small=cv2.resize(img,img_dim,interpolation=cv2.INTER_AREA)
#imggray=cv2.equalizeHist(cv2.cvtColor(img_small,cv2.COLOR_BGR2GRAY))
#face_cascade=cv2.CascadeClassifier(cv2.data.haarcascades+'haarcascade_frontalface_default.xml')
#faces = face_cascade.detectMultiScale(imggray,1.05,10) #1.05 - ideal 1.25 para melhor acuracidade
#for (x, y, w, h) in faces:
#   img_faces=cv2.rectangle(imggray,(x,y),(x+w,y+h),(255,255,0),2)
#
#cv2.imshow('faces',img_faces)
#cv2.waitKey(0)
#cv2.destroyAllWindows()
#
#---mais[Animal,Person,Vehicle]
##https://towardsdatascience.com/detecting-animals-in-the-backyard-practical-application-of-deep-learning-c030d3263ba8
##https://github.com/gaiar/animal-detector/tree/dev

