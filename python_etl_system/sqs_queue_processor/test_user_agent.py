from etl.sqs_queue_processor.processors.common.user_agent import extract_user_agent


def test_empty_event():
    user_agent = extract_user_agent({})

    assert(user_agent['ua_browser_family'] == 'Other or Unknown')
    assert(user_agent['ua_browser_version'] == '')
    assert(user_agent['ua_os_family'] == 'Other or Unknown')
    assert(user_agent['ua_os_version'] == '')
    assert(user_agent['ua_device_family'] == 'Other or Unknown')
    assert(user_agent['ua_device_brand'] is None)
    assert(user_agent['ua_device_model'] is None)
    assert(user_agent['ua_is_mobile'] == False)
    assert(user_agent['ua_is_tablet'] == False)
    assert(user_agent['ua_is_touch'] == False)
    assert(user_agent['ua_is_pc'] == False)
    assert(user_agent['ua_is_bot'] == False)
    assert(user_agent['ua_is_internal_ua'] == False)


def test_empty_ua():
    user_agent = extract_user_agent({'request': {'user-agent': ''}})

    assert(user_agent['ua_browser_family'] == 'Other or Unknown')
    assert(user_agent['ua_browser_version'] == '')
    assert(user_agent['ua_os_family'] == 'Other or Unknown')
    assert(user_agent['ua_os_version'] == '')
    assert(user_agent['ua_device_family'] == 'Other or Unknown')
    assert(user_agent['ua_device_brand'] is None)
    assert(user_agent['ua_device_model'] is None)
    assert(user_agent['ua_is_mobile'] == False)
    assert(user_agent['ua_is_tablet'] == False)
    assert(user_agent['ua_is_touch'] == False)
    assert(user_agent['ua_is_pc'] == False)
    assert(user_agent['ua_is_bot'] == False)
    assert (user_agent['ua_is_internal_ua'] == False)


def test_full():
    user_agent = extract_user_agent({'request': {'user-agent': 'Mozilla/5.0 (Linux; Android 5.0; SM-G870W Build/LRX21T; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/46.0.2490.76 Mobile Safari/5Th37.36'}})

    assert(user_agent['ua_browser_family'] == 'Chrome Mobile')
    assert(user_agent['ua_browser_version'] == '46.0.2490')
    assert(user_agent['ua_os_family'] == 'Android')
    assert(user_agent['ua_os_version'] == '5')
    assert(user_agent['ua_device_family'] == 'Samsung SM-G870W')
    assert(user_agent['ua_device_brand'] == 'Samsung')
    assert(user_agent['ua_device_model'] == 'SM-G870W')
    assert(user_agent['ua_is_mobile'] == True)
    assert(user_agent['ua_is_tablet'] == False)
    assert(user_agent['ua_is_touch'] == True)
    assert(user_agent['ua_is_pc'] == False)
    assert(user_agent['ua_is_bot'] == False)
    assert (user_agent['ua_is_internal_ua'] == False)


def test_internal_ua():
    user_agent = extract_user_agent({'request': {'user-agent': 'internal_ua/83 CFNetwork/609.1.4 Darwin/13.0.0'}})

    assert(user_agent['ua_browser_family'] == 'CFNetwork')
    assert(user_agent['ua_browser_version'] == '609.1.4')
    assert(user_agent['ua_os_family'] == 'iOS')
    assert(user_agent['ua_os_version'] == '6.1')
    assert(user_agent['ua_device_family'] == 'iOS-Device')
    assert(user_agent['ua_device_brand'] == 'Apple')
    assert(user_agent['ua_device_model'] == 'iOS-Device')
    assert(user_agent['ua_is_mobile'] == False)
    assert(user_agent['ua_is_tablet'] == False)
    assert(user_agent['ua_is_touch'] == True)
    assert(user_agent['ua_is_pc'] == False)
    assert(user_agent['ua_is_bot'] == False)
    assert (user_agent['ua_is_internal_ua'] == True)

