#encoding: utf-8
import sys
def scoreClickAUC(num_clicks, num_impressions, predicted_ctr):  
    i_sorted = sorted(range(len(predicted_ctr)),key=lambda i: predicted_ctr[i],  
                      reverse=True)  
    auc_temp = 0.0  
    click_sum = 0.0  
    old_click_sum = 0.0  
    no_click = 0.0  
    no_click_sum = 0.0  
  
    # treat all instances with the same predicted_ctr as coming from the  
    # same bucket  
    last_ctr = predicted_ctr[i_sorted[0]] + 1.0  
    #print last_ctr
    #print predicted_ctr #
    #print i_sorted #
    for i in range(len(predicted_ctr)):  
        #print i
        if last_ctr != predicted_ctr[i_sorted[i]]:  
            auc_temp += (click_sum+old_click_sum) * no_click / 2.0  
            old_click_sum = click_sum  
            no_click = 0.0  
            last_ctr = predicted_ctr[i_sorted[i]]  
        #print 'auc_temp:%d click_sum:%d old_click_sum:%d no_click:%d last_ctr:%d' %(auc_temp,click_sum,old_click_sum,no_click,last_ctr)
        no_click += num_impressions[i_sorted[i]] - num_clicks[i_sorted[i]]  
        no_click_sum += num_impressions[i_sorted[i]] - num_clicks[i_sorted[i]]  
        click_sum += num_clicks[i_sorted[i]]  
    #print click_sum, old_click_sum, no_click

    auc_temp += (click_sum+old_click_sum) * no_click / 2.0  
    auc = click_sum * no_click_sum
    auc = 0 if auc == 0 else auc_temp / auc 
    return auc

if __name__ == '__main__':

    '''输入为文本格式，每行一个测试样本，点击以0表示，不点击以1表示。
            每行包括四个字段，分布是:是否点击 点击概率 不点击概率 原始点击数
            调用方式为：cat output/exp5.out | python ./liblinear_modified/GanjiAuc.py。
           ganjauc和kddcup_auc都改造自kddcup 2012 track2 的评分程序'''
            
    num_clicks = []
    num_impressions = []
    predicted_ctr = []

    for line in sys.stdin:
        item = line.rstrip('\n').split(' ')
        num_clicks.append(1 if float(item[3]) <= 0 else 0)
        num_impressions.append(1.0)
        #由于0表示点击1表示不点击与-1表示不点击1表示点击的顺序相反，所有predict_ctr取反，如果取item[1]也行，但须用1减去最终结果才是需要的auc
        predicted_ctr.append(float(item[2]))
    print scoreClickAUC(num_clicks, num_impressions, predicted_ctr)

