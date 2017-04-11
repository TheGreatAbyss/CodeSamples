from user_agents import parse
from etl.services.logging import get_logger


def extract_ua_string(event):
    return event.get('request', {}).get('user-agent', '')


def extract_user_agent_dict(user_agent_string):
    if not user_agent_string:
        return {}

    user_agent = parse(user_agent_string)

    properties = {
        'ua_browser_family': user_agent.browser.family if user_agent.browser.family != 'Other' else 'Other or Unknown',
        'ua_browser_version': user_agent.browser.version_string,
        'ua_os_family': user_agent.os.family if user_agent.os.family != 'Other' else 'Other or Unknown',
        'ua_os_version': user_agent.os.version_string,
        'ua_device_family': user_agent.device.family if user_agent.device.family != 'Other' else 'Other or Unknown',
        'ua_device_brand': getattr(getattr(user_agent, 'device', ''), 'brand', ''),
        'ua_device_model': getattr(getattr(user_agent, 'device', ''), 'model', ''),
        'ua_is_mobile': user_agent.is_mobile,
        'ua_is_tablet': user_agent.is_tablet,
        'ua_is_touch': user_agent.is_touch_capable,
        'ua_is_pc': user_agent.is_pc,
        'ua_is_bot': user_agent.is_bot
    }

    if "internal_ua" not in user_agent_string.lower():
        properties['ua_is_internal'] = False
    else:
        properties['ua_is_internal'] = True

    return properties


def extract_user_agent(event):
    user_agent_string = extract_ua_string(event)
    return extract_user_agent_dict(user_agent_string)