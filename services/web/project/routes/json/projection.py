from project import app
from project.utils import do_query, render_template, debug_timer
import chajda
import chajda.tsquery
from flask import request
import itertools
import logging
import datetime
import json
import time


import numpy as np
default_projectionvector = np.array(
    [ 0.02957594,  0.50386024, -2.6844428 ,  0.6105701 ,  2.1159103 ,
     -1.84395   , -2.579988  , -0.03845882,  0.581807  ,  2.0521002 ,
      0.57888997,  2.045848  ,  1.2586529 ,  0.05062222,  0.77035975,
     -0.07423502,  2.436877  ,  2.097585  , -2.9644618 , -3.0558    ,
      2.4757    ,  2.79049   , -0.35434967,  1.3205721 ,  1.9617298 ,
     -0.07862997,  0.5482297 , -1.35866   , -0.1098249 , -2.9464078 ,
      0.35745955,  2.06783   , -2.087224  ,  1.266543  ,  0.19132555,
     -1.54833   ,  0.66981   ,  2.02947   , -0.32838506, -0.588573  ,
      2.154276  , -0.06101902, -1.401586  ,  1.176679  ,  0.7171189 ,
     -0.91807485, -0.21014398, -2.991881  , -0.5503142 ,  2.348786  ])
default_projectionvector = np.array([0.028314702212810516, 0.026004919782280922, 0.10509494692087173, -0.08844860643148422, -0.03349178656935692, -0.017841054126620293, 0.0062125069089233875, -0.023376546800136566, -0.04746994003653526, -0.0072877611964941025, 0.046394698321819305, -0.09406375139951706, -0.011867477558553219, -0.0662667527794838, -0.11640488356351852, -0.041695486754179, -0.003703614231199026, 0.03890782222151756, -0.12616170942783356, 0.07024913281202316, 0.1802424043416977, -0.00015930966765154153, 0.04822658374905586, -0.01533216331154108, 0.009916120208799839, 0.07506781071424484, -0.01855788193643093, 0.06781988590955734, -0.14706920087337494, 0.021265896037220955, 0.00756651908159256, -0.0696517825126648, -0.030903249979019165, -0.11274110525846481, -0.07837319374084473, 0.0706871971487999, -0.02839435078203678, 0.006132869981229305, 0.004938148427754641, 0.06897477060556412, 0.05611169710755348, 0.011469247750937939, -0.07212086021900177, -0.04117778316140175, 0.058461304754018784, -0.00728774955496192, 0.0802847221493721, -0.05392139032483101, 0.020270293578505516, -0.005774447228759527, 0.007566514424979687, 0.02086765319108963, -0.09239115566015244, -0.03508474677801132, 0.013619733043015003, -0.11385618150234222, 0.06849689036607742, -0.07845283299684525, 0.11361723393201828, 0.019872067496180534, -0.015133035369217396, 0.03687681257724762, -0.09557706117630005, -0.02496950700879097, 0.02911117859184742, -0.030146600678563118, -0.04599646106362343, -0.030266057699918747, -0.10939591377973557, 0.028513824567198753, -0.008880703710019588, -0.01740298792719841, 0.06547028571367264, -0.06053214147686958, 0.004181495867669582, -0.07598376274108887, 0.0662667527794838, 0.03970429673790932, 0.008124049752950668, 0.02735893242061138, -0.02186325192451477, 0.04030165821313858, -0.06272244453430176, -0.027677519246935844, 0.0575055256485939, 0.014017968438565731, 0.02461109310388565, -0.0181198101490736, 0.01043382752686739, 0.05885953828692436, 0.04834606125950813, -0.01712423376739025, -0.04133708402514458, -0.02134554274380207, -0.000199116300791502, 0.07514745742082596, 0.07853248715400696, -0.03874853253364563, 0.060850732028484344, -0.06594817340373993, 0.02481021359562874, 0.10027626156806946, 0.10768347978591919, 0.1287502646446228, 0.036398936063051224, 0.11090919375419617, 0.1663040667772293, 0.02532792091369629, 0.039345890283584595, 0.04575752094388008, -0.030903246253728867, -0.03667769581079483, -0.014177259989082813, 0.01943400129675865, 0.10915696620941162, 0.017801228910684586, -0.074550099670887, -0.009995770640671253, -0.055713459849357605, -0.021823430433869362, 0.02254025824368, -0.019553476944565773, -0.008203696459531784, 0.05292579531669617, 0.005973566323518753, 0.08454587310552597, 0.08044402301311493, -0.0821564570069313, -0.056191347539424896, 0.015730390325188637, 0.02843416854739189, -0.06706323474645615, -0.04774870350956917, -0.06845707446336746, 0.03839011490345001, -0.04372650012373924, -0.0833909809589386, 0.06097019836306572, 0.0017522475682199001, -0.01031434815376997, 0.0017522430280223489, 0.015491454862058163, 0.08717423677444458, 0.10457723587751389, 0.0608905553817749, -0.060213543474674225, 0.07271821051836014, -0.13783007860183716, -0.04631505161523819, -0.0011947156162932515, -0.09597530961036682, 0.06471363455057144, 0.06260297447443008, -0.018717173486948013, 0.004420436453074217, 0.05244791507720947, -0.012186067178845406, -0.038708705455064774, -0.02938993275165558, 0.1105906218290329, 0.041934434324502945, 0.029389947652816772, 0.010433832183480263, 0.017960529774427414, 0.011349780485033989, 0.03715558722615242, 0.04607610031962395, -0.014456025324761868, -0.10047537088394165, 0.04750976338982582, -0.0037832572124898434, 0.016925105825066566, -0.011708191595971584, 0.008840872906148434, -0.14356470108032227, 0.0018318890361115336, 0.1173606589436531, 0.13173703849315643, -0.061607375741004944, -0.012066601775586605, 0.02341637574136257, 0.002230133395642042, -0.02090747281908989, 0.0018717193743214011, 0.054200153797864914, -0.05702764168381691, -0.037792760878801346, 0.05579310655593872, 0.0014734867727383971, -0.008601933717727661, -0.037952058017253876, -0.030226239934563637, 0.009597531519830227, 0.007885098457336426, -0.0036239640321582556, 0.12480770796537399, 0.04285037890076637, 0.02429249882698059, 0.10613035410642624, 0.07359433174133301, -0.03516439348459244, -0.07267838716506958, -0.03309355303645134, -0.07192172855138779, -0.05220896750688553, -0.029708530753850937, 0.03723522275686264, 0.02568633295595646, 0.046952228993177414, -0.07606340199708939, 0.05603204295039177, -0.04774870723485947, -0.01533215120434761, -0.0005177119164727628, -0.07224033027887344, 0.007924934849143028, 0.03524404019117355, -0.05105408653616905, 0.0657888799905777, -0.04253178834915161, 0.02839435636997223, -0.011986956931650639, -0.004818681161850691, -0.07873159646987915, -0.03169972822070122, 0.005893914494663477, -0.03532369062304497, -0.04420439153909683, -0.05125319957733154, -0.03169972822070122, 0.07574481517076492, 0.005455855745822191, -0.06443486362695694, -0.011150658130645752, -0.00911964476108551, 0.029828006401658058, 0.04659382253885269, -0.054040856659412384, -0.07188191264867783, 0.008163874968886375, 0.001672597136348486, -0.005296562798321247, -0.05463821813464165, -0.03273514285683632, 0.014854267239570618, 0.01206660270690918, 0.023296909406781197, 0.03811135143041611, -0.021106600761413574, -0.06754111498594284, 0.04723099619150162, -0.08494410663843155, 0.042452145367860794, -0.04340791329741478, -0.0117878383025527, -0.035602450370788574, 0.014256920665502548, 0.004420430865138769, 0.05583292618393898, -0.06754112243652344, 0.03118201531469822, -0.002070835791528225, 0.06116931885480881, 0.07084649801254272, -0.024690741673111916, 0.10979414731264114, 0.03635910525918007, 0.027478402480483055, -0.01652686484158039, -0.033332500606775284, -0.03361126407980919, -0.02325708232820034, -0.01469497475773096, 0.0461159273982048, -0.07658111304044724, -0.0575055293738842, 0.09689123928546906, -0.052487727254629135, -0.006730217486619949, -0.010354184545576572, -0.020190656185150146, -0.017641931772232056, 0.1386265605688095, 0.07184208184480667, -0.14854268729686737, -0.0003185896493960172, -0.04101848229765892, 0.003185903886333108, 0.11692259460687637, -0.07486870139837265, 0.04277073219418526, -0.06387732923030853, 0.1414540410041809, -0.032297082245349884, 0.08482463657855988, -0.030943071469664574, 0.053881559520959854, 0.07490852475166321, 0.0385892391204834, 0.0013938259799033403])

def get_projection(time_lo_def, time_hi_def, terms, lang, filter_hosts, pos_words, neg_words, granularity, fast=True):
    '''
    FIXME:
    does not support OR or NOT clause
    '''
    granularity = 'day'

    if len(filter_hosts) == 0:
        host = ''
        clause_host = ''
    else:
        host = '_host'
        #clause_host = 'and unsurt("contextvector.host_surt") = ANY(:filter_hosts)' 
        clause_host = 'and "contextvector.host_surt" = host_surt(:filter_host1)' 

    if len(terms) == 0:
        focus = ''
        clause_focus = ''
    else:
        focus = 'focus'
        clause_focus = 'and "contextvector.focus" = :focus'


    if fast:
        projectionvector = default_projectionvector
    if not fast:
        projectionvector, badwords = chajda.embeddings.get_embedding(lang='en', max_n=50000, storage_dir='./embeddings').make_projectionvector(neg_words, pos_words)

    sql = (f'''
    select
        timestamp_published_{granularity} as x,
        "hostpath_surt" AS counts,
        "avg(context)" <#> :projectionvector :: vector as sentiment
    from contextvector_{focus}{granularity}{host}
    where timestamp_published_{granularity} >= :time_lo
      and timestamp_published_{granularity} <  :time_hi
      {clause_focus}
      {clause_host}
    order by x asc
    ''')
    sql = (f'''
    with results as (
        select
            x,
            coalesce(counts, 0) as counts,
            sentiment
        from (
            select generate_series(:time_lo, :time_hi, '1 {granularity}'::interval) as x
        ) as x
        left outer join (
            select
                timestamp_published_{granularity} as x,
                /*sum*/("hostpath_surt") AS counts,
                /*avg*/("avg(context)" <#> :projectionvector :: vector) as sentiment
            from contextvector_{focus}{granularity}lang{host}
            where timestamp_published_{granularity} >= :time_lo
              and timestamp_published_{granularity} <  :time_hi
              and "contextvector.language" = :lang
              {clause_focus}
              {clause_host}
            /*group by x*/
        ) t_sentiment using(x)
        --order by x asc
     )
     select *
     from results
     where
        x >= (select min(x) from results where counts>0)
    ''')
    sql = (f'''
    with results as (
        select
            timestamp_published_{granularity} as x,
            sum("hostpath_surt") AS counts,
            avg("avg(context)" <#> :projectionvector :: vector) as sentiment
        from contextvector_{focus}{granularity}lang{host}
        where timestamp_published_{granularity} >= :time_lo
          and timestamp_published_{granularity} <  :time_hi
          and "contextvector.language" = :lang
          {clause_focus}
          {clause_host}
        group by x
        order by x asc
    )
    select *
    from results
    right outer join ( 
        select generate_series((select min(x) from results), (select max(x) from results), '1 {granularity}'::interval) as x
    ) as xs using (x)
    ''')

    bind_params = {}
    bind_params['time_lo'] = time_lo_def
    bind_params['time_hi'] = time_hi_def
    bind_params['projectionvector'] = projectionvector.tolist()
    bind_params['focus'] = terms[0] if len(terms) > 0 else None
    bind_params['filter_hosts'] = filter_hosts
    bind_params['filter_host1'] = filter_hosts[0] if len(filter_hosts)>0 else None
    bind_params['lang'] = lang
    res = do_query('sentiments', sql, bind_params)
    data = {
        'xs': [ row.x for row in res ],
        'sentiments': [ row.sentiment for row in res ],
        'counts': [ row.counts for row in res ],
        }
    return data
