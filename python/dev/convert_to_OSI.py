def convert_to_OSI(options):
    OSI_list = []
    for option in options:
        year_index = next(i for i, c in enumerate(option) if c.isdigit())
        underlying = option[:year_index].ljust(6)  # pad with spaces to the right
        date = option[year_index:year_index+8]
        cp = option[year_index+8]
        strike = option[year_index+9:]
        strike_split = strike.split('.')
        strike_dollar = strike_split[0].zfill(5)
        strike_decimal = strike_split[1].ljust(3, '0')
        OSI = underlying + date + cp + strike_dollar + strike_decimal
        OSI_list.append(OSI)
    return OSI_list

# Test the function
options = []
convert_to_OSI(options)
