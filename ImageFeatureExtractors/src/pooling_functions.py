import numpy as np
import timm
from torch.utils.data import Dataset, DataLoader
from tqdm.auto import tqdm
import torch.nn.functional as F
import torch

# --------------------------------------
# pooling
# --------------------------------------


def gem(x, p=3, eps=1e-6):
    return F.avg_pool2d(x.clamp(min=eps).pow(p), (x.size(-2), x.size(-1))).pow(1./p)
    # return F.lp_pool2d(F.threshold(x, eps, eps), p, (x.size(-2), x.size(-1))) # alternative




def channel_wise_weighting(X):
    b, K, w, h = X.shape
    XX = X.view(X.size(0), X.size(1), -1)
    xx_sum = XX.sum(axis=2)
    weights = F.softmax(xx_sum, dim=-1)
    weights = weights.unsqueeze(2).unsqueeze(2).expand(X.shape[0],  X.shape[1], X.shape[2], X.shape[-1])
    X = X*weights
    output = X.mean([-2, -1])
    return output

def spatial_wise_weighting(X):
    b, K, w, h = X.shape
    x_sum = X.sum(axis=1)
    x_sum_1 = F.softmax(x_sum, dim=1)
    x_sum_2 = F.softmax(x_sum_1, dim=2)
    weights = x_sum_2.unsqueeze(1).expand(X.shape[0],  X.shape[1], X.shape[2], X.shape[-1])
    X = X*weights
    output = X.mean([-2, -1])
    return output

class ChannelDataset(Dataset):
    def __init__(self, data, device):
        self.data = data
        self.device = device

    def __getitem__(self, index):
        data = self.data[index]
        if self.device=='gpu':
            data = torch.tensor(data).to(self.device)
        return data
    def __len__(self):
        return len(self.data)



def channel_pooling(features, device, batch_size, pooling_type='sum', sigmoid=False,
                    follow_convnext_architecture=False,
                    model_name='convnext_large_in22k'):
    imageclef_dataset = ChannelDataset(features, device)
    dataloader = DataLoader(imageclef_dataset, batch_size=batch_size, shuffle=False)
    batchImageArray = np.empty([0, features.shape[1]], float)
    print(f"Data loader length: {len(dataloader)}")

    if follow_convnext_architecture:
        model = timm.create_model(model_name=model_name, pretrained=True)
        model_norm = model.head[1]
        model_norm = model_norm.to(device)

    for batch in tqdm(dataloader):
        batch = torch.tensor(batch, dtype=float, requires_grad=False).to(device)
        if pooling_type =='sum':
            if sigmoid:
                batch = F.sigmoid(batch)
            if follow_convnext_architecture:
                imageFeatures =  torch.sum(batch.view(batch.size(0), batch.size(1), -1), dim=2)
                imageFeatures = imageFeatures.unsqueeze(-1).unsqueeze(-1)
                imageFeatures = model_norm(imageFeatures.float())
                imageFeatures = imageFeatures.squeeze(-1).squeeze(-1)
            else:
                imageFeatures = torch.sum(batch.view(batch.size(0), batch.size(1), -1), dim=2)


        elif  pooling_type =='max':
            if sigmoid:
                batch = F.sigmoid(batch)

            if follow_convnext_architecture:
                imageFeatures = torch.max(batch.view(batch.size(0), batch.size(1), -1), dim=2)[0]
                imageFeatures = imageFeatures.unsqueeze(-1).unsqueeze(-1)
                imageFeatures = model_norm(imageFeatures.float())
                imageFeatures = imageFeatures.squeeze(-1).squeeze(-1)
            else:
                imageFeatures = torch.max(batch.view(batch.size(0), batch.size(1), -1), dim=2)[0]
        elif  pooling_type =='gem':
            if sigmoid:
                batch = F.sigmoid(batch)
            if follow_convnext_architecture:
                imageFeatures = gem(batch.float())
                imageFeatures = model_norm(imageFeatures)
                imageFeatures = imageFeatures.squeeze(-1).squeeze(-1)
            else:
                imageFeatures = gem(batch.float()).squeeze(-1).squeeze(-1)

        elif  pooling_type =='channel_wise_weighting':
            if sigmoid:
                batch = F.sigmoid(batch)

            if follow_convnext_architecture:
                imageFeatures = channel_wise_weighting(batch.float())
                imageFeatures = imageFeatures.unsqueeze(-1).unsqueeze(-1)
                imageFeatures = model_norm(imageFeatures)
                imageFeatures = imageFeatures.squeeze(-1).squeeze(-1)
            else:
                imageFeatures = channel_wise_weighting(batch.float()).squeeze(-1).squeeze(-1)



        elif pooling_type == 'spatial_wise_weighting':
            if sigmoid:
                batch = F.sigmoid(batch)

            if follow_convnext_architecture:
                imageFeatures = spatial_wise_weighting(batch.float())
                imageFeatures = imageFeatures.unsqueeze(-1).unsqueeze(-1)

                imageFeatures = model_norm(imageFeatures)
                imageFeatures = imageFeatures.squeeze(-1).squeeze(-1)
            else:
                imageFeatures = spatial_wise_weighting(batch.float()).squeeze(-1).squeeze(-1)

        imageFeatures = imageFeatures.cpu().detach().numpy()
        batchImageArray = np.append(batchImageArray, imageFeatures, axis=0)

    return batchImageArray

