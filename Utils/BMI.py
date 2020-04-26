# the BMI for children is calculated whenever is possible 
def replace_bmi_child(p):
    if p['Age'] is  None or p['Child_weight'] is None or p['Child_height'] is None:
        return (p['Patient_number'], p['Age'], p['Sex'], None)

    if p['Age']<18:
        return (p['Patient_number'], p['Age'], p['Sex'], p['Child_weight'] / (p['Child_height']/100)**2)
    else:
        return (p['Patient_number'], p['Age'], p['Sex'], p['Adult_BMI'])